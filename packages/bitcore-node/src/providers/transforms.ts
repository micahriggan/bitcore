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
    const [inputs, outputs] = await Promise.all([
      CoinModel.collection
        .find({
          chain: transaction.chain,
          network: transaction.network,
          spentTxid: transaction.txid
        })
        .addCursorFlag('noCursorTimeout', true)
        .toArray(),
      CoinModel.collection
        .find({
          chain: transaction.chain,
          network: transaction.network,
          mintTxid: transaction.txid
        })
        .addCursorFlag('noCursorTimeout', true)
        .toArray()
    ]);

    transaction.inputs = inputs;
    transaction.outputs = outputs;
    const wallet = this.wallet._id!.toString();
    const sending = transaction.inputs.some(input => {
      let contains = false;
      for (let inputWallet of input.wallets) {
        if (inputWallet.equals(wallet)) {
          contains = true;
        }
      }
      return contains;
    });

    const totalInputs = transaction.inputs.reduce((total, input) => {
      return total + input.value;
    }, 0);
    let totalOutputs = 0;

    for (let output of transaction.outputs) {
      totalOutputs += output.value;
      if (sending) {
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
      let receiving = false;
      for (let outputWallet of output.wallets) {
        if (outputWallet.equals(wallet)) {
          receiving = true;
        }
      }
      if (receiving) {
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
    const fee = totalInputs - totalOutputs;
    if (sending && fee > 0) {
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

    done();
  }
}
