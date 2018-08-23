import { AdapterType, ConvertBlockParams, ConvertTxParams, Bucket } from '.';
import { Ethereum } from '../types/namespaces/Ethereum';
import { ITransaction } from '../models/transaction';
import { BN } from 'bn.js';
export class EthereumAdapter implements AdapterType<Ethereum.Block, Ethereum.Transaction> {
  convertBlock(params: ConvertBlockParams<Ethereum.Block>) {
    const { chain, network, block } = params;
    const {header} = block;
    return {
      chain,
      network,
      height: new BN(header.number).toNumber(),
      hash: block.header.hash().toString('hex'),
      version: 1,
      merkleRoot: block.header.transactionsTrie.toString('hex'),
      time: new Date(header.timestamp.readUInt32BE(0) * 1000),
      timeNormalized: new Date(header.timestamp.readUInt32BE(0) * 1000),
      nonce: Number(header.nonce.toString('hex')),
      previousBlockHash: header.parentHash.toString('hex'),
      nextBlockHash: '',
      transactionCount: block.transactions.length,
      size: block.raw.length,
      reward: 3,
      processed: false,
      bits: 0,
      bucket: {
        gasLimit: Number(header.gasLimit),
        gasUsed: Number(header.gasUsed),
        stateRoot: header.stateRoot
      }
    };
  }

  convertTx(params: ConvertTxParams<Ethereum.Transaction>) {
    const { chain, network, tx, block } = params;
    const convertedTx: Bucket<ITransaction> = {
      chain,
      network,
      wallets: [],
      txid: tx.hash().toString('hex'),
      size: tx.data.length,
      fee: tx.getUpfrontCost().toString(),
      bucket: { gasLimit: tx.gasLimit.toString('hex'), gasPrice: tx.gasPrice.toString('hex'), nonce: tx.nonce.toString('hex') }
    };
    if (block) {
      convertedTx.blockHeight = block.height;
      convertedTx.blockHash = block.hash;
      convertedTx.blockTime = block.time;
      convertedTx.blockTimeNormalized = block.timeNormalized;
    }

    const inputs = [
      {
        mintTxid: tx.hash().toString('hex'),
        mintIndex: 0,
        bucket: {}
      }
    ];

    const outputs = [
      {
        value: Number(tx.value.toString('hex')),
        address: '0x' + tx.to.toString('hex'),
        bucket: {}
      }
    ];

    return { ...convertedTx, inputs, outputs };
  }
}
