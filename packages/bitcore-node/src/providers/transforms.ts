import { CoinModel } from '../models/coin';
import { Transform } from 'stream';
import { IWallet } from '../models/wallet';
import { MongoBound } from '../models/base';

export class ListTransactionsStream extends Transform {
  constructor(private wallet: MongoBound<IWallet>) {
    super({ objectMode: true });
  }

  async _transform(transaction, _, done) {
    const self = this;
    transaction.inputs = await CoinModel.collection
      .find(
        {
          chain: transaction.chain,
          network: transaction.network,
          spentTxid: transaction.txid
        },
        { batchSize: 100 }
      )
      .addCursorFlag('noCursorTimeout', true)
      .toArray();
    transaction.outputs = await CoinModel.collection
      .find(
        {
          chain: transaction.chain,
          network: transaction.network,
          mintTxid: transaction.txid
        },
        { batchSize: 100 }
      )
      .addCursorFlag('noCursorTimeout', true)
      .toArray();

    const wallet = this.wallet._id!.toString();
    const totalInputs = transaction.inputs.reduce((total, input) => {
      return total + input.value;
    }, 0);
    const totalOutputs = transaction.outputs.reduce((total, output) => {
      return total + output.value;
    }, 0);
    const fee = totalInputs - totalOutputs;
    const sending = transaction.inputs.some((input) => {
      let contains = false;
      for (let inputWallet of input.wallets) {
        if (inputWallet.equals(wallet)) {
          contains = true;
        }
      }
      return contains;
    });

    if (sending) {
      for (let output of transaction.outputs) {
        self.push(
          JSON.stringify({
            txid: transaction.txid,
            category: 'send',
            satoshis: -output.value,
            height: transaction.blockHeight,
            address: output.address,
            outputIndex: output.vout,
            blockTime: transaction.blockTimeNormalized
          }) + '\n'
        );
      }
      if (fee > 0) {
        self.push(
          JSON.stringify({
            txid: transaction.txid,
            category: 'fee',
            satoshis: -fee,
            height: transaction.blockHeight,
            blockTime: transaction.blockTimeNormalized
          }) + '\n'
        );
      }
      return done();
    }

    for (let output of transaction.outputs) {
      let contains = false;
      for (let outputWallet of output.wallets) {
        if (outputWallet.equals(wallet)) {
          contains = true;
        }
      }
      if (contains) {
        self.push(
          JSON.stringify({
            txid: transaction.txid,
            category: 'receive',
            satoshis: output.value,
            height: transaction.blockHeight,
            address: output.address,
            outputIndex: output.vout,
            blockTime: transaction.blockTimeNormalized
          }) + '\n'
        );
      }
    }
    done();
  }
}
