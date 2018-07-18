import { LoggifyClass } from '../decorators/Loggify';
import { BaseModel } from './base';
import { ObjectID } from 'mongodb';

const Chain = require('../chain');

export type ICoin = {
  network: string;
  chain: string;
  mintTxid: string;
  mintIndex: number;
  mintHeight: number;
  coinbase: boolean;
  value: number;
  address: string;
  script: Buffer;
  wallets: Set<ObjectID>;
  spentTxid: string;
  spentHeight: number;
};

@LoggifyClass
class Coin extends BaseModel<ICoin> {
  constructor() {
    super('coins');
  }

  allowedPaging = [
    { key: 'mintHeight' as 'mintHeight', type: 'number' as 'number' },
    { key: 'spentHeight' as 'spentHeight', type: 'number' as 'number' }
  ];

  onConnect() {
    this.collection.createIndex({ mintTxid: 1 });
    this.collection.createIndex(
      { mintTxid: 1, mintIndex: 1 },
      { partialFilterExpression: { spentHeight: { $lt: 0 } } }
    );
    this.collection.createIndex({ address: 1 });
    this.collection.createIndex({ mintHeight: 1, chain: 1, network: 1 });
    this.collection.createIndex({ spentTxid: 1 }, { sparse: true });
    this.collection.createIndex({ spentHeight: 1, chain: 1, network: 1 });
    this.collection.createIndex({ wallets: 1, spentHeight: 1 }, { sparse: true });
  }

  async totalValue(params: { query: any }) {
    let { query } = params;
    let [{ total }] = await this.collection
      .aggregate<{ total: number }>([
        { $match: query },
        {
          $group: {
            _id: null,
            total: { $sum: '$value' }
          }
        },
        { $project: { _id: false } }
      ])
      .toArray();
    return total;
  }

  async getTotalSent(query) {
    query = Object.assign(query, { spentHeight: { $gt: -1 } });
    const total = await this.totalValue({ query });
    return { totalSent: total };
  }

  async getTotalReceived(query) {
    query = Object.assign(query, { mintHeight: { $gt: -1 } });
    const total = await this.totalValue({ query });
    return { totalReceived: total };
  }

  async getBalance(params: { query: any }) {
    let { query } = params;
    query = Object.assign(query, { spentHeight: { $lt: 0 } });
    const total = await this.totalValue({ query });
    return { balance: total };
  }

  _apiTransform(coin: ICoin, options: { object: boolean }) {
    let sbuf = coin.script ? coin.script.buffer : Buffer.from('');
    const script = Chain[coin.chain].lib.Script.fromBuffer(sbuf);
    let transform = {
      txid: coin.mintTxid,
      coinbase: coin.coinbase,
      vout: coin.mintIndex,
      spentTxid: coin.spentTxid,
      address: coin.address,
      script: {
        type: script.classify(),
        asm: script.toASM()
      },
      value: coin.value
    };
    if (options && options.object) {
      return transform;
    }
    return JSON.stringify(transform);
  }
}
export let CoinModel = new Coin();
