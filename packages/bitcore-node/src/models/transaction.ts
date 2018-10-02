import { CoinModel, ICoin } from './coin';
import { WalletAddressModel, IWalletAddress } from './walletAddress';
import { TransformOptions } from '../types/TransformOptions';
import { LoggifyClass } from '../decorators/Loggify';
import { BaseModel } from './base';
import logger from '../logger';
import config from '../config';
import { ObjectId } from 'mongodb';
import { VerboseTransaction } from '../adapters';
import { partition } from '../utils/partition';

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
  mintOps?: Array<CoinMintOp>;
  spendOps?: Array<CoinSpendOp>;
  txOps?: Array<TxOp>;
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

export type CoinSpendOp = {
  updateOne: {
    filter: {
      mintTxid: string;
      mintIndex: number;
      spentHeight: { $lt: 0 };
      chain: string;
      network: string;
    };
    update: { $set: { spentTxid: string; spentHeight: number } };
  };
};

export type TxOp = {
  updateOne: {
    filter: { txid: string; chain: string; network: string };
    update: {
      $set: Partial<ITransaction>;
    };
    upsert: boolean;
    forceServerObjectId: boolean;
  };
};

@LoggifyClass
export class Transaction extends BaseModel<ITransaction> {
  // use this to find mints
  // clear it when the incoming mints have decreased in size
  // which would indicate a new batch
  mintMap = {};
  lastIndex = 0;

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
    const { txs, height, parentChain, mempoolTime, forkHeight, chain, network, initialSyncComplete } = params;

    // get the batches from the params
    let { mintOps = new Array<CoinMintOp>(), spendOps = new Array<CoinSpendOp>(), txOps = new Array<TxOp>() } = params;
    let newMintOps = this.getMintOps(params);
    logger.debug('Mint batch size', mintOps.length);
    const allMintOps = mintOps.concat(newMintOps);

    const newSpendOps = this.getSpendOps({
      txs,
      height,
      parentChain,
      mempoolTime,
      forkHeight,
      chain,
      network,
      mintOps: allMintOps
    });
    logger.debug('Spend batch size', spendOps.length);

    let newTxOps = new Array<TxOp>();
    if (mintOps) {
      newTxOps = await this.getTxOps(params);
      //const allTxOps = txOps.concat(newTxOps);
      logger.debug('Tx batch size', txOps.length);
      if (initialSyncComplete) {
        let mintOpsAddresses = mintOps.map(mintOp => mintOp.updateOne.update.$set.address);
        let walletAddresses = await WalletAddressModel.collection
          .find({ address: { $in: mintOpsAddresses }, chain, network }, { batchSize: 100 })
          .toArray();
        newMintOps = await this.getWalletMintOps({ ...params, walletAddresses });
      }
    }
    return {
      txOps: newTxOps,
      mintOps: newMintOps,
      spendOps: newSpendOps
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
    await this.processBatchOps(batchesOfOperations);
  }

  async processBatchOps(params: { mintOps: Array<CoinMintOp>; spendOps: Array<CoinSpendOp>; txOps: Array<TxOp> }) {
    let { mintOps, spendOps, txOps } = params;
    let writeOps = new Array<Promise<any>>();
    if (mintOps.length) {
      logger.debug('Writing Mints', mintOps.length);
      let mintBatches: CoinMintOp[] | CoinMintOp[][] = mintOps.slice();
      mintBatches = partition(mintBatches, mintBatches.length / 10);
      writeOps.concat(mintBatches.map(batch => CoinModel.collection.bulkWrite(batch)));
    }
    if (spendOps.length) {
      logger.debug('Writing Spends', spendOps.length);
      let spendBatches: CoinSpendOp[] | CoinSpendOp[][] = spendOps.slice();
      spendBatches = partition(spendBatches, spendBatches.length / 10);
      writeOps.push(CoinModel.collection.bulkWrite(spendOps, { ordered: false }));
    }
    await Promise.all(writeOps);

    if (txOps.length) {
      logger.debug('Writing Transactions', txOps.length);
      let txBatches: TxOp[] | TxOp[][] = txOps.slice();
      txBatches = partition(txBatches, txOps.length / 10);
      const writeTxs = txBatches.map(tx => TransactionModel.collection.bulkWrite(tx, { ordered: false }));
      await Promise.all(writeTxs);
    }
  }

  async getTxOps(params: {
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
              txid: txids[index],
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
    let { mintOps = new Array<CoinMintOp>() } = params;
    let spendOps = new Array<CoinSpendOp>();
    if (parentChain && forkHeight && height < forkHeight) {
      return spendOps;
    }

    if (mintOps.length < Object.keys(this.mintMap).length) {
      this.mintMap = {};
      this.lastIndex = 0;
    }
    for (let i = this.lastIndex; i < mintOps.length; i++) {
      const mintOp = mintOps[i];
      const { mintTxid, mintIndex } = mintOp.updateOne.filter;
      if (this.mintMap[mintTxid] && this.mintMap[mintTxid][mintIndex]) {
        continue;
      }
      this.mintMap[mintTxid] = this.mintMap[mintIndex] || {};
      this.mintMap[mintTxid][mintIndex] = { index: i };
      this.lastIndex = i;
    }

    let sameBlockSpends = 0;
    for (let tx of txs) {
      if (tx.bucket.coinbase) {
        continue;
      }
      let txid = tx.txid;
      for (let input of tx.inputs) {
        let sameBatchSpend = this.mintMap[input.mintTxid] && this.mintMap[input.mintTxid][input.mintIndex];
        if (sameBatchSpend) {
          sameBlockSpends++;
          const mintOp = mintOps[sameBatchSpend.index];
          mintOp.updateOne.update.$set.spentHeight = height;
          mintOp.updateOne.update.$set.spentTxid = txid;
          if (config.pruneSpentScripts && height > 0) {
            delete mintOp.updateOne.update.$set.script;
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
