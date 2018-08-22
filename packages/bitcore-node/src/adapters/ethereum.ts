import { AdapterType, ConvertBlockParams, ConvertTxParams, Bucket } from '.';
import { Ethereum } from '../types/namespaces/Ethereum';
import { ITransaction } from '../models/transaction';
export class EthereumAdapter implements AdapterType<Ethereum.Block, Ethereum.Transaction> {
  convertBlock(params: ConvertBlockParams<Ethereum.Block>) {
    const { chain, network, block } = params;
    return {
      chain,
      network,
      height: block.number,
      hash: block.mixHash,
      version: 1,
      merkleRoot: block.transactionsTrie,
      time: new Date(block.timestamp),
      timeNormalized: new Date(block.timestamp),
      nonce: block.nonce,
      previousBlockHash: block.parentHash,
      nextBlockHash: '',
      transactionCount: 0,
      size: block.raw.length,
      reward: 3,
      processed: false,
      bits: 0,
      bucket: {
        gasLimit: block.gasLimit,
        gasUsed: block.gasUsed,
        stateRoot: block.stateRoot
      }
    };
  }

  convertTx(params: ConvertTxParams<Ethereum.Transaction>) {
    const { chain, network, tx, block } = params;
    const convertedTx: Bucket<ITransaction> = {
      chain,
      network,
      wallets: [],
      txid: tx.hash,
      size: tx.data.length,
      fee: -1,
      bucket: { gasLimit: tx.gasLimit, gasPrice: tx.gasPrice, nonce: tx.nonce }
    };
    if (block) {
      convertedTx.blockHeight = block.height;
      convertedTx.blockHash = block.hash;
      convertedTx.blockTime = block.time;
      convertedTx.blockTimeNormalized = block.timeNormalized;
    }
    const inputs = [
      {
        mintTxid: tx.hash,
        mintIndex: 0,
        bucket: {}
      }
    ];

    const outputs = [
      {
        value: tx.value,
        address: tx.to,
        bucket: {}
      }
    ];

    return { ...convertedTx, inputs, outputs };
  }
}
