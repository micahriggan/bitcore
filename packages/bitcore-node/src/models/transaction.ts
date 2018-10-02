import { CoinModel, ICoin, SpentHeightIndicators } from './coin';
import { WalletAddressModel } from './walletAddress';
import { partition } from '../utils/partition';
import { ObjectID } from 'bson';
import { TransformOptions } from '../types/TransformOptions';
import { LoggifyClass } from '../decorators/Loggify';
import { Bitcoin } from '../types/namespaces/Bitcoin';
import { BaseModel, MongoBound } from './base';
import logger from '../logger';
import config from '../config';
import { BulkWriteOpResultObject } from 'mongodb';
import { StreamingFindOptions, Storage } from '../services/storage';
import LRU = require('lru-cache');

const Chain = require('../chain');
const mintCache = new LRU<string, CoinAggregation>(100000);

type CoinAggregation = { total: number; wallets: Array<ObjectID> };
type CoinGroup = { [txid: string]: CoinAggregation };
export type ITransaction = {
  txid: string;
  chain: string;
  network: string;
  blockHeight?: number;
  blockHash?: string;
  blockTime?: Date;
  blockTimeNormalized?: Date;
  coinbase: boolean;
  fee: number;
  size: number;
  locktime: number;
  value: number;
  raw: string;
  wallets: ObjectID[];
};

@LoggifyClass
export class Transaction extends BaseModel<ITransaction> {
  constructor() {
    super('transactions');
  }

  allowedPaging = [{ key: 'blockHeight' as 'blockHeight', type: 'number' as 'number' }];

  onConnect() {
    this.collection.createIndex({ txid: 1 });
    this.collection.createIndex({ blockHeight: 1, chain: 1, network: 1 });
    this.collection.createIndex({ blockHash: 1 });
    this.collection.createIndex({ blockTimeNormalized: 1, chain: 1, network: 1 });
    this.collection.createIndex({ wallets: 1 }, { sparse: true });
  }

  async batchImport(params: {
    txs: Array<Bitcoin.Transaction>;
    height: number;
    mempoolTime?: Date;
    blockTime?: Date;
    blockHash?: string;
    blockTimeNormalized?: Date;
    parentChain?: string;
    forkHeight?: number;
    chain: string;
    network: string;
    initialSyncComplete: boolean;
    mintOps?: Array<any>;
  }) {
    let mintOps = await this.getMintOps(params);
    logger.debug('Minting Coins', mintOps.length);

    params.mintOps = mintOps || [];
    let spendOps = this.getSpendOps(params);
    logger.debug('Spending Coins', spendOps.length);
    if (mintOps.length) {
      mintOps = partition(mintOps, mintOps.length / config.maxPoolSize);
      mintOps = mintOps.map((mintBatch: Array<any>) => {
        return CoinModel.collection.bulkWrite(mintBatch, { ordered: false });
      });
    }
    if (spendOps.length) {
      spendOps = partition(spendOps, spendOps.length / config.maxPoolSize);
      spendOps = spendOps.map((spendBatch: Array<any>) =>
        CoinModel.collection.bulkWrite(spendBatch, { ordered: false })
      );
    }
    const coinOps = mintOps.concat(spendOps);
    await Promise.all(coinOps);

    let txs: Promise<BulkWriteOpResultObject>[] = [];
    if (mintOps) {
      let txOps = await this.addTransactions(params);
      logger.debug('Writing Transactions', txOps.length);
      const txBatches = partition(txOps, txOps.length / config.maxPoolSize);
      txs = txBatches.map((txBatch: Array<any>) =>
        this.collection.bulkWrite(txBatch, { ordered: false, j: false, w: 0 })
      );
    }

    await Promise.all(txs);
  }

  async addTransactions(params: {
    txs: Array<Bitcoin.Transaction>;
    height: number;
    blockTime?: Date;
    blockHash?: string;
    blockTimeNormalized?: Date;
    parentChain?: string;
    forkHeight?: number;
    initialSyncComplete: boolean;
    chain: string;
    network: string;
    mintOps?: Array<any>;
  }) {
    let { blockHash, blockTime, blockTimeNormalized, chain, height, network, txs } = params;
    let txids = txs.map(tx => tx._hash) as string[];
    const neededTxids = txids.filter(txid => !mintCache.has(txid));
    const mintedPromise = CoinModel.collection.find({ mintTxid: { $in: txids }, chain, network }).toArray();
    const spentPromise = CoinModel.collection.find({ spentTxid: { $in: neededTxids }, chain, network }).toArray();
    const [minted, spent] = await Promise.all([mintedPromise, spentPromise]);
    const groupedMints = minted.reduce<CoinGroup>((agg, current) => {
      if (!agg[current.mintTxid]) {
        agg[current.mintTxid] = {
          total: current.value,
          wallets: current.wallets || []
        };
      } else {
        agg[current.mintTxid].total += current.value;
        agg[current.mintTxid].wallets.push(...current.wallets);
      }
      return agg;
    }, {});

    const groupedSpends = spent.reduce<CoinGroup>((agg, current) => {
      if (!agg[current.spentTxid]) {
        agg[current.spentTxid] = {
          total: current.value,
          wallets: current.wallets || []
        };
      } else {
        agg[current.spentTxid].total += current.value;
        agg[current.spentTxid].wallets.push(...current.wallets);
      }
      return agg;
    }, {});

    let txOps = txs.map((tx, index) => {
      const txid = tx._hash!;
      const minted = groupedMints[txid] || {};
      const cached = mintCache.has(txid) ? mintCache.get(txid) : undefined;
      const spent = cached || groupedSpends[txid] || {};
      const mintedWallets = minted.wallets || [];
      const spentWallets = spent.wallets || [];
      const wallets = mintedWallets.concat(spentWallets);
      let fee = 0;
      if (minted.total && spent.total) {
        fee = spent.total - minted.total;
      }

      return {
        updateOne: {
          filter: { txid: txids[index], chain, network },
          update: {
            $set: {
              chain,
              network,
              blockHeight: height,
              blockHash,
              blockTime,
              blockTimeNormalized,
              coinbase: tx.isCoinbase(),
              value: minted.total,
              fee,
              raw: tx.toBuffer().toString('hex'),
              size: tx.toBuffer().length,
              locktime: tx.nLockTime,
              wallets
            }
          },
          upsert: true,
          forceServerObjectId: true
        }
      };
    });
    return txOps;
  }

  async getMintOps(params: {
    txs: Array<Bitcoin.Transaction>;
    height: number;
    parentChain?: string;
    forkHeight?: number;
    initialSyncComplete: boolean;
    chain: string;
    network: string;
    mintOps?: Array<any>;
  }): Promise<any> {
    let { chain, height, network, txs, parentChain, forkHeight, initialSyncComplete } = params;
    let mintOps = new Array<any>();
    let parentChainCoins = new Array<ICoin>();
    if (parentChain && forkHeight && height < forkHeight) {
      parentChainCoins = await CoinModel.collection
        .find({
          chain: parentChain,
          network,
          mintHeight: height,
          spentHeight: { $gt: SpentHeightIndicators.unspent, $lt: forkHeight }
        })
        .toArray();
    }
    for (let tx of txs) {
      tx._hash = tx.hash;
      let txid = tx._hash;
      let isCoinbase = tx.isCoinbase();
      for (let [index, output] of tx.outputs.entries()) {
        let parentChainCoin = parentChainCoins.find(
          (parentChainCoin: ICoin) => parentChainCoin.mintTxid === txid && parentChainCoin.mintIndex === index
        );
        if (parentChainCoin) {
          continue;
        }
        let address = '';
        let scriptBuffer = output.script && output.script.toBuffer();
        if (scriptBuffer) {
          address = output.script.toAddress(network).toString(true);
          if (address === 'false' && output.script.classify() === 'Pay to public key') {
            let hash = Chain[chain].lib.crypto.Hash.sha256ripemd160(output.script.chunks[0].buf);
            address = Chain[chain].lib.Address(hash, network).toString(true);
          }
        }

        mintOps.push({
          updateOne: {
            filter: {
              mintTxid: txid,
              mintIndex: index,
              spentHeight: { $lt: SpentHeightIndicators.minimum },
              chain,
              network
            },
            update: {
              $set: {
                chain,
                network,
                mintHeight: height,
                coinbase: isCoinbase,
                value: output.satoshis,
                address,
                script: scriptBuffer,
                spentHeight: SpentHeightIndicators.unspent,
                wallets: []
              }
            },
            upsert: true,
            forceServerObjectId: true
          }
        });
      }
    }

    if (initialSyncComplete) {
      let mintOpsAddresses = mintOps.map(mintOp => mintOp.updateOne.update.$set.address);
      let wallets = await WalletAddressModel.collection
        .find({ address: { $in: mintOpsAddresses }, chain, network }, { batchSize: 100 })
        .toArray();
      if (wallets.length) {
        mintOps = mintOps.map(mintOp => {
          let transformedWallets = wallets
            .filter(wallet => wallet.address === mintOp.updateOne.update.$set.address)
            .map(wallet => wallet.wallet);
          mintOp.updateOne.update.$set.wallets = transformedWallets;
          return mintOp;
        });
      }
    }

    for (let mongoMintOp of mintOps) {
      const mintOp = mongoMintOp.updateOne.update.$set;
      let old = mintCache.get(mintOp.mintTxid);
      if (old) {
        const newValue = old.total + mintOp.value;
        const newWallets = old.wallets.concat(mintOp.wallets);
        mintCache.set(mintOp.mintTxid, { total: newValue, wallets: newWallets });
      } else {
        mintCache.set(mintOp.mintTxid, { total: mintOp.value, wallets: mintOp.wallets });
      }
    }

    return mintOps;
  }

  getSpendOps(params: {
    txs: Array<Bitcoin.Transaction>;
    height: number;
    parentChain?: string;
    forkHeight?: number;
    chain: string;
    network: string;
    mintOps?: Array<any>;
  }): Array<any> {
    let { chain, network, height, txs, parentChain, forkHeight, mintOps = [] } = params;
    let spendOps: any[] = [];
    if (parentChain && forkHeight && height < forkHeight) {
      return spendOps;
    }
    let mintMap = {};
    for (let mintOp of mintOps) {
      mintMap[mintOp.updateOne.filter.mintTxid] = mintMap[mintOp.updateOne.filter.mintIndex] || {};
      mintMap[mintOp.updateOne.filter.mintTxid][mintOp.updateOne.filter.mintIndex] = mintOp;
    }
    for (let tx of txs) {
      if (tx.isCoinbase()) {
        continue;
      }
      let txid = tx._hash;
      for (let input of tx.inputs) {
        let inputObj = input.toObject();
        let sameBlockSpend = mintMap[inputObj.prevTxId] && mintMap[inputObj.prevTxId][inputObj.outputIndex];
        if (sameBlockSpend) {
          sameBlockSpend.updateOne.update.$set.spentHeight = height;
          sameBlockSpend.updateOne.update.$set.spentTxid = txid;
          if (config.pruneSpentScripts && height > 0) {
            delete sameBlockSpend.updateOne.update.$set.script;
          }
          continue;
        }
        const updateQuery: any = {
          updateOne: {
            filter: {
              mintTxid: inputObj.prevTxId,
              mintIndex: inputObj.outputIndex,
              spentHeight: { $lt: SpentHeightIndicators.minimum },
              chain,
              network
            },
            update: { $set: { spentTxid: txid, spentHeight: height } }
          }
        };
        if (config.pruneSpentScripts && height > 0) {
          updateQuery.updateOne.update.$unset = { script: null };
        }
        spendOps.push(updateQuery);
      }
    }
    return spendOps;
  }

  getTransactions(params: { query: any; options: StreamingFindOptions<ITransaction> }) {
    let originalQuery = params.query;
    const { query, options } = Storage.getFindOptions(this, params.options);
    const finalQuery = Object.assign({}, originalQuery, query);
    return this.collection.find(finalQuery, options).addCursorFlag('noCursorTimeout', true);
  }

  _apiTransform(tx: Partial<MongoBound<ITransaction>>, options: TransformOptions): Partial<ITransaction> | string {
    let transform = {
      _id: tx._id,
      txid: tx.txid,
      network: tx.network,
      blockHeight: tx.blockHeight,
      blockHash: tx.blockHash,
      blockTime: tx.blockTime,
      blockTimeNormalized: tx.blockTimeNormalized,
      coinbase: tx.coinbase,
      locktime: tx.locktime,
      raw: tx.raw,
      size: tx.size,
      fee: tx.fee
    };
    if (options && options.object) {
      return transform;
    }
    return JSON.stringify(transform);
  }
}
export let TransactionModel = new Transaction();
