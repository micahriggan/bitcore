import { CoinModel, ICoin } from './coin';
import { WalletAddressModel, IWalletAddress } from './walletAddress';
import { partition } from '../utils/partition';
import { TransformOptions } from '../types/TransformOptions';
import { LoggifyClass } from '../decorators/Loggify';
import { BaseModel } from './base';
import logger from '../logger';
import config from '../config';
import { ObjectId } from 'mongodb';
import { VerboseTransaction } from '../adapters';

export type ITransaction = {
  txid: string;
  chain: string;
  network: string;
  blockHeight?: number;
  blockHash?: string;
  blockTime?: Date;
  blockTimeNormalized?: Date;
  fee: number;
  size: number;
  wallets: ObjectId[];
};

type BatchImportParams = {
  txs: Array<VerboseTransaction>;
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
  spendOps?: Array<any>;
  txOps?: Array<any>;
};
type BatchImportQueryBuilderParams = BatchImportParams & {
  parentChainCoins: Array<ICoin>;
};

export type CoinMintOp = {
  updateOne: {
    filter: { mintTxid: string; mintIndex: number; spentHeight: { $lt: 0 }; chain: string; network: string };
    update: {
      $set: Partial<ICoin>;
    };
    upsert: true;
    forceServerObjectId: true;
  };
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

  async getBatchOps(params: BatchImportQueryBuilderParams) {
    const { chain, network, initialSyncComplete } = params;
    let { mintOps = [], spendOps = [], txOps = [] } = params;
    const newMints = this.getMintOps(params);
    const allMintOps = mintOps.concat(newMints);
    logger.debug('Mint batch size', mintOps.length);

    const spendParams = Object.assign({}, params, { mintOps: allMintOps });
    const newSpends = this.getSpendOps(spendParams);
    //const allSpenOps = spendOps.concat(newSpends);
    logger.debug('Spend batch size', spendOps.length);

    let newTxOps = new Array<any>();
    if (mintOps) {
      newTxOps = await this.addTransactions(params);
      //const allTxOps = txOps.concat(newTxOps);
      logger.debug('Tx batch size', txOps.length);
      if (initialSyncComplete) {
        let mintOpsAddresses = mintOps.map(mintOp => mintOp.updateOne.update.$set.address);
        let walletAddresses = await WalletAddressModel.collection
          .find({ address: { $in: mintOpsAddresses }, chain, network }, { batchSize: 100 })
          .toArray();
        mintOps = await this.getWalletMintOps({ ...params, walletAddresses });
      }
    }
    return {
      txOps: newTxOps,
      mintOps: newMints,
      spendOps: newSpends
    };
  }

  async batchImport(params: {
    txs: Array<VerboseTransaction>;
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
    parentChainCoins?: Array<ICoin>;
  }) {
    let { parentChain, forkHeight, network, height, parentChainCoins } = params;
    parentChainCoins = parentChainCoins || [];
    if (parentChain && forkHeight && height < forkHeight && parentChainCoins.length === 0) {
      parentChainCoins = await CoinModel.collection
        .find({
          chain: parentChain,
          network,
          mintHeight: height,
          spentHeight: { $gt: -2, $lt: forkHeight }
        })
        .toArray();
    }

    let batchesOfOperations = await this.getBatchOps({ ...params, parentChainCoins });
    await this.processBatches(batchesOfOperations);
  }

  async processBatches(params: { mintOps: Array<any>; spendOps: Array<any>; txOps: Array<any> }) {
    let { mintOps, spendOps, txOps } = params;
    if (mintOps.length) {
      logger.debug('Writing Mints', mintOps.length);
      mintOps = partition(mintOps, mintOps.length / config.maxPoolSize);
      mintOps = mintOps.map((mintBatch: Array<any>) => CoinModel.collection.bulkWrite(mintBatch, { ordered: false }));
    }
    if (spendOps.length) {
      logger.debug('Writing Spends', spendOps.length);
      spendOps = partition(spendOps, spendOps.length / config.maxPoolSize);
      spendOps = spendOps.map((spendBatch: Array<any>) =>
        CoinModel.collection.bulkWrite(spendBatch, { ordered: false })
      );
    }
    const coinOps = mintOps.concat(spendOps);
    await Promise.all(coinOps);

    if (mintOps && txOps.length) {
      logger.debug('Writing Transactions', txOps.length);
      txOps = partition(txOps, txOps.length / config.maxPoolSize);
      txOps = txOps.map(txBatch => TransactionModel.collection.bulkWrite(txBatch, { ordered: false }));
    }

    await Promise.all(txOps);
  }

  async addTransactions(params: {
    txs: Array<VerboseTransaction>;
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
    let { chain, network, height, blockHash, blockTime, blockTimeNormalized, txs, initialSyncComplete } = params;
    let txids = txs.map(tx => tx.txid);

    type TaggedCoin = {
      _id: string;
      wallets: Array<ObjectId>;
    };
    let mintWallets: Array<TaggedCoin> = [];
    let spentWallets: Array<TaggedCoin> = [];

    if (initialSyncComplete) {
      mintWallets = await CoinModel.collection
        .aggregate<TaggedCoin>([
          { $match: { mintTxid: { $in: txids }, chain, network } },
          { $unwind: '$wallets' },
          { $group: { _id: '$mintTxid', wallets: { $addToSet: '$wallets' } } }
        ])
        .toArray();

      spentWallets = await CoinModel.collection
        .aggregate<TaggedCoin>([
          { $match: { spentTxid: { $in: txids }, chain, network } },
          { $unwind: '$wallets' },
          { $group: { _id: '$spentTxid', wallets: { $addToSet: '$wallets' } } }
        ])
        .toArray();
    }
    const txWallets = mintWallets.concat(spentWallets);

    let txOps = txs.map((tx, index) => {
      let wallets = new Array<ObjectId>();
      if (initialSyncComplete) {
        for (let wallet of txWallets.filter(wallet => wallet._id === txids[index])) {
          for (let walletMatch of wallet.wallets) {
            if (!wallets.find(wallet => wallet.toHexString() === walletMatch.toHexString())) {
              wallets.push(walletMatch);
            }
          }
        }
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
              size: tx.size,
              wallets,
              ...tx.bucket
            }
          },
          upsert: true,
          forceServerObjectId: true
        }
      };
    });
    return txOps;
  }

  getMintOps(params: BatchImportQueryBuilderParams): Array<CoinMintOp> {
    let { chain, height, network, txs, parentChainCoins } = params;
    let mintOps = new Array<CoinMintOp>();

    for (let tx of txs) {
      let txid = tx.txid;
      for (let [index, output] of tx.outputs.entries()) {
        let parentChainCoin = parentChainCoins.find(
          (parentChainCoin: ICoin) => parentChainCoin.mintTxid === txid && parentChainCoin.mintIndex === index
        );
        if (parentChainCoin) {
          continue;
        }
        mintOps.push({
          updateOne: {
            filter: { mintTxid: txid, mintIndex: index, spentHeight: { $lt: 0 }, chain, network },
            update: {
              $set: {
                chain,
                network,
                mintHeight: height,
                value: output.value,
                address: output.address,
                spentHeight: -2,
                wallets: new Array<ObjectId>(),
                ...output.bucket
              }
            },
            upsert: true,
            forceServerObjectId: true
          }
        });
      }
    }
    return mintOps;
  }

  getWalletMintOps(params: BatchImportQueryBuilderParams & { walletAddresses: IWalletAddress[] }) {
    const { walletAddresses, mintOps = [] } = params;
    if (walletAddresses.length && mintOps) {
      return mintOps.map(mintOp => {
        let transformedWallets = walletAddresses
          .filter(walletAddress => walletAddress.address === mintOp.updateOne.update.$set.address)
          .map(walletAddress => walletAddress.wallet);
        mintOp.updateOne.update.$set.wallets = transformedWallets;
        return mintOp;
      });
    }
    return mintOps;
  }

  getSpendOps(params: {
    txs: Array<VerboseTransaction>;
    height: number;
    parentChain?: string;
    mempoolTime?: Date;
    forkHeight?: number;
    chain: string;
    network: string;
    mintOps?: Array<CoinMintOp>;
  }) {
    const { chain, network, height, txs, parentChain, forkHeight } = params;
    let { mintOps = [] } = params;
    let spendOps: any[] = [];
    if (parentChain && forkHeight && height < forkHeight) {
      return spendOps;
    }
    let mintMap = {};
    for (let mintOp of mintOps) {
      mintMap[mintOp.updateOne.filter.mintTxid] = mintMap[mintOp.updateOne.filter.mintIndex] || {};
      mintMap[mintOp.updateOne.filter.mintTxid][mintOp.updateOne.filter.mintIndex] = mintOp;
    }
    let sameBlockSpends = 0;
    for (let tx of txs) {
      if (tx.bucket.coinbase) {
        continue;
      }
      let txid = tx.txid;
      for (let input of tx.inputs) {
        let sameBlockSpend = mintMap[input.mintTxid] && mintMap[input.mintTxid][input.mintIndex];
        if (sameBlockSpend) {
          sameBlockSpends++;
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
              mintTxid: input.mintTxid,
              mintIndex: input.mintIndex,
              spentHeight: { $lt: 0 },
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
    logger.debug(`Processed ${sameBlockSpends} spends in memory`);
    return spendOps;
  }

  getTransactions(params: { query: any }) {
    let query = params.query;
    return this.collection.find(query).addCursorFlag('noCursorTimeout', true);
  }

  _apiTransform(tx: ITransaction, options: TransformOptions) {
    let keys = Object.keys(tx).filter(k => k != '_id');
    let transform = {};
    for (let key of keys) {
      transform[key] = tx[key];
    }
    if (options && options.object) {
      return transform;
    }
    return JSON.stringify(transform);
  }
}
export let TransactionModel = new Transaction();
