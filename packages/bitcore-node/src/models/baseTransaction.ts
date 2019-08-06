import { ObjectID } from 'bson';
import { TransformOptions } from '../types/TransformOptions';
import { BaseModel, MongoBound } from './base';
import { StreamingFindOptions, Storage, StorageService } from '../services/storage';

export type ITransaction = {
  txid: string;
  chain: string;
  network: string;
  blockHeight?: number;
  blockHash?: string;
  blockTime?: Date;
  blockTimeNormalized?: Date;
  fee: number;
  value: number;
  wallets: ObjectID[];
};

export abstract class BaseTransaction<T extends ITransaction> extends BaseModel<T> {
  constructor(storage?: StorageService) {
    super('transactions', storage);
  }

  allowedPaging = [
    { key: 'blockHash' as 'blockHash', type: 'string' as 'string' },
    { key: 'blockHeight' as 'blockHeight', type: 'number' as 'number' },
    { key: 'blockTimeNormalized' as 'blockTimeNormalized', type: 'date' as 'date' },
    { key: 'txid' as 'txid', type: 'string' as 'string' }
  ];

  onConnect() {
    this.collection.createIndex({ txid: 1 }, { background: true });
    this.collection.createIndex({ chain: 1, network: 1, blockHeight: 1 }, { background: true });
    this.collection.createIndex({ blockHash: 1 }, { background: true });
    this.collection.createIndex({ chain: 1, network: 1, blockTimeNormalized: 1 }, { background: true });
    this.collection.createIndex(
      { wallets: 1, blockTimeNormalized: 1 },
      { background: true, partialFilterExpression: { 'wallets.0': { $exists: true } } }
    );
    this.collection.createIndex(
      { wallets: 1, blockHeight: 1 },
      { background: true, partialFilterExpression: { 'wallets.0': { $exists: true } } }
    );
  }

  getTransactions(params: { query: any; options: StreamingFindOptions<T> }) {
    let originalQuery = params.query;
    const { query, options } = Storage.getFindOptions(this, params.options);
    const finalQuery = Object.assign({}, originalQuery, query);
    return this.collection.find(finalQuery, options).addCursorFlag('noCursorTimeout', true);
  }

  abstract _apiTransform(tx: T | Partial<MongoBound<T>>, options?: TransformOptions): any | string;
}
