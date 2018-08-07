import logger from '../logger';
import config from '../config';
import { LoggifyClass } from '../decorators/Loggify';
import { Storage } from './storage';
import { TransactionModel } from '../models/transaction';
import { CoinModel } from '../models/coin';

@LoggifyClass
export class FeeService {
  constructor() {}

  async start(chain: string, network: string) {
    if (Storage.connectionStatus === 'never') {
      await Storage.start({});
    }
    logger.info('Starting Fee Service for ', chain, network);
    await this.findTxsMissingFee(chain, network);
  }

  async startConfigured() {
    for (let chain of Object.keys(config.chains)) {
      for (let network of Object.keys(config.chains[chain])) {
        const chainConfig = config.chains[chain][network];
        if (chainConfig && chainConfig.services && chainConfig.services.fee) {
          await Fee.start(chain, network);
        }
      }
    }
  }

  async findTxsMissingFee(chain: string, network: string) {
    TransactionModel.collection.parallelCollectionScan({ numCursors: 5 }, async (_, cursors) => {
      for (let cursor of cursors) {
        let txids = new Array<string>();
        while (await cursor.hasNext()) {
          let tx = await cursor.next();
          if (tx && tx.chain === chain && tx.network === network) {
            if (!tx.fee) {
              txids.push(tx.txid);
            }
          }
          if (txids.length % 1000 === 0) {
            logger.info('Processing 1000 transaction fees');
            const fees = await this.getFeeMapping(chain, network, txids);
            await this.updateFees(chain, network, fees);
            txids = [];
          }
        }
        if (txids.length > 0) {
          // finish out the last batch
          const fees = await this.getFeeMapping(chain, network, txids);
          await this.updateFees(chain, network, fees);
        }
      }
    });
  }

  async getFeeMapping(chain: string, network: string, txids: Array<string>) {
    type TxidTotal = { _id: string; total: number };
    let fees = {};
    let bulk = new Array<any>();
    for (let txid of txids) {
      bulk.push({ chain, network, spentTxid: txid });
      bulk.push({ chain, network, mintTxid: txid });
    }

    const [mintsAndSpends] = await CoinModel.collection
      .aggregate<{ mints: Array<TxidTotal>; spends: Array<TxidTotal> }>([
        { $match: { $or: bulk } },
        {
          $facet: {
            spends: [{ $group: { _id: '$spentTxid', total: { $sum: '$value' } } }],
            mints: [{ $group: { _id: '$mintTxid', total: { $sum: '$value' } } }]
          }
        }
      ])
      .toArray();

    logger.debug(JSON.stringify(mintsAndSpends));

    const mintMap = mintsAndSpends.mints.reduce((obj, mint) => {
      obj[mint._id] = mint.total;
      return obj;
    }, {});

    const spendMap = mintsAndSpends.spends.reduce((obj, spend) => {
      obj[spend._id] = spend.total;
      return obj;
    }, {});

    logger.debug(JSON.stringify(mintMap));
    logger.debug(JSON.stringify(spendMap));

    for (let txid of txids) {
      if (fees[txid] === undefined) {
        fees[txid] = mintMap[txid] - (spendMap[txid] || 0);
      }
    }
    return fees;
  }

  async updateFees(chain, network, fees: { [txid: string]: number }) {
    let bulkOps = new Array<any>();
    for (let txid of Object.keys(fees)) {
      bulkOps.push({ updateOne: { filter: { chain, network, txid }, update: { $set: { fee: fees[txid] } } } });
    }
    logger.debug(`Writing ${bulkOps.length} fees`);
    await TransactionModel.collection.bulkWrite(bulkOps);
  }

  stop() {}
}

// TOOO: choose a place in the config for the API timeout and include it here
export const Fee = new FeeService();
