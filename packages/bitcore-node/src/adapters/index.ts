export type Bucket<T> = { bucket: any } & T;
import { ITransaction } from '../models/transaction';
import { IBlock } from '../models/block';
import { BitcoinAdapter } from './bitcoin';
import { EthereumAdapter } from "./ethereum";

export type VerboseTransaction = Bucket<ITransaction> & {
  inputs: Bucket<{ mintTxid: string; mintIndex: number }>[];
  outputs: Bucket<{ value: number; address: string }>[];
};
export type ConvertBlockParams<B> = { chain: string; network: string; block: B };
export type ConvertTxParams<T> = { chain: string; network: string; tx: T; block?: IBlock };
export interface AdapterType<B, T> {
  convertBlock: (params: ConvertBlockParams<B>) => Bucket<IBlock>;
  convertTx: (params: ConvertTxParams<T>) => VerboseTransaction;
}
const adapters: { [adapterName: string]: AdapterType<any, any> } = {
  BTC: new BitcoinAdapter(),
  BCH: new BitcoinAdapter(),
  ETH: new EthereumAdapter()
};

class AdapterProvider {
  get(adapterName: string) {
    return adapters[adapterName];
  }
  register<B, T>(adapterName: string, adapter: AdapterType<B, T>) {
    adapters[adapterName] = adapter;
  }
  convertBlock<B>(params: ConvertBlockParams<B>) {
    return this.get(params.chain).convertBlock(params);
  }
  convertTx<T>(params: ConvertTxParams<T>) {
    return this.get(params.chain).convertTx(params);
  }
}
export const Adapters = new AdapterProvider();
