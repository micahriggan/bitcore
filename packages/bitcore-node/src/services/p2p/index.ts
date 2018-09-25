export type VerboseTransaction = ITransaction & {
  inputs: Bucket<{ mintTxid: string; mintIndex: number }>[];
  outputs: Bucket<{ value: number; address: string }>[];
};
export type ConvertBlockParams<B> = { chain: string; network: string; block: B };
export type ConvertTxParams<T> = { chain: string; network: string; tx: T; block?: IBlock };
import { Bucket } from '../../types/namespaces/ChainAdapter';
import { EthP2pService } from './eth';
export interface AdapterType<B, T> {
  convertBlock: (params: ConvertBlockParams<B>) => Bucket<IBlock>;
  convertTx: (params: ConvertTxParams<T>) => VerboseTransaction;
}

import { ITransaction } from '../../models/transaction';
import { IBlock } from '../../models/block';
import config from '../../config';
import { BitcoreP2pService } from './bitcoin';
interface IP2PConstructor<B, T> {
  new (params: { chain: string; network: string; chainConfig: string }): IP2P<B, T>;
}

export interface IP2P<B, T> {
  start();
  convertBlock(block: B): Bucket<IBlock>;
  convertTransaction(tx: T): Bucket<VerboseTransaction>;
}

export class P2pProvider {
  private p2pClasses: { [p2pClass: string]: IP2PConstructor<any, any> } = {
    BTC: BitcoreP2pService,
    BCH: BitcoreP2pService,
    ETH: EthP2pService
  };

  register<B, T>(chain: string, p2pClass: IP2PConstructor<B, T>) {
    this.p2pClasses[chain] = p2pClass;
  }

  get(params: { chain: string }) {
    return this.p2pClasses[params.chain];
  }

  start(params: { chain: string; network: string; chainConfig: string }) {
    const p2pClass = this.get(params);
    const p2p = new p2pClass(params);
    p2p.start();
  }

  startConfiguredChains() {
    for (let chain of Object.keys(config.chains)) {
      for (let network of Object.keys(config.chains[chain])) {
        const chainConfig = config.chains[chain][network];
        this.start({ chain, network, chainConfig });
      }
    }
  }
}
export const P2P = new P2pProvider();
