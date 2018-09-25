import { ConvertTxParams, ConvertBlockParams, AdapterType } from '..';
import { Bitcoin } from '../../../types/namespaces/Bitcoin';
import { ITransaction } from '../../../models/transaction';
import { Chain } from '../../../chain';
import { Bucket } from '../../../types/namespaces/ChainAdapter';
import { IBlock } from '../../../models/block';

export type IBitcoinTransaction = ITransaction & {
  locktime: number;
  coinbase: boolean;
};

export type IBitcoinBlock = IBlock & {};

export class BitcoinAdapter implements AdapterType<Bitcoin.Block, Bitcoin.Transaction> {
  convertBlock(params: ConvertBlockParams<Bitcoin.Block>) {
    const { chain, network, block } = params;
    const header = block.header.toObject();
    const blockTime = header.time * 1000;
    return {
      chain,
      network,
      height: -1,
      timeNormalized: new Date(blockTime),
      nextBlockHash: '',
      processed: false,
      hash: block.hash,
      version: Number(header.version),
      previousBlockHash: header.prevHash,
      merkleRoot: header.merkleRoot,
      time: new Date(blockTime),
      bits: Number(header.bits),
      nonce: Number(header.nonce),
      transactionCount: block.transactions.length,
      size: block.toBuffer().length,
      reward: block.transactions[0].outputAmount,
      bucket: {}
    };
  }

  convertTx(params: ConvertTxParams<Bitcoin.Transaction>) {
    const { chain, network, tx, block } = params;
    const convertedTx: Bucket<ITransaction> = {
      chain,
      network,
      wallets: [],
      txid: tx.hash,
      size: tx.toBuffer().length,
      fee: -1,
      bucket: {
        locktime: tx.nLockTime,
        coinbase: tx.isCoinbase()
      }
    };
    if (block) {
      convertedTx.blockHeight = block.height;
      convertedTx.blockHash = block.hash;
      convertedTx.blockTime = block.time;
      convertedTx.blockTimeNormalized = block.timeNormalized;
    }
    const inputs = tx.inputs.map(i => {
      const inputObj = i.toObject();
      return {
        mintTxid: inputObj.prevTxId,
        mintIndex: inputObj.outputIndex,
        bucket: {}
      };
    });

    const outputs = tx.outputs.map(o => {
      let scriptBuffer = o.script && o.script.toBuffer();
      let address;
      if (scriptBuffer) {
        address = o.script.toAddress(network).toString(true);
        if (address === 'false' && o.script.classify() === 'Pay to public key') {
          let hash = Chain[chain].lib.crypto.Hash.sha256ripemd160(o.script.chunks[0].buf);
          address = Chain[chain].lib.Address(hash, network).toString(true);
        }
      }
      return {
        value: o.satoshis,
        address,
        bucket: { script: o.script }
      };
    });

    return { ...convertedTx, inputs, outputs };
  }
}
