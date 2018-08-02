import { ICoin } from '../models/coin';
import { Transform } from 'stream';
import { TransactionModel } from '../models/transaction';

export class ListTransactionsStream extends Transform {
  seen: { [txid: string]: boolean } = {};

  constructor() {
    super({
      objectMode: true
    });
  }

  async writeTxToStream(coin: ICoin) {
    if (coin.spentTxid) {
      if (!this.seen[coin.spentTxid]) {
        const spentTx = await TransactionModel.collection.findOne({ txid: coin.spentTxid });
        if (spentTx) {
          const fee = spentTx.fee;
          this.push(
            JSON.stringify({
              txid: coin.spentTxid,
              category: 'fee',
              satoshis: -fee,
              height: coin.spentHeight
            }) + '\n'
          );
        }
      }
      this.push(
        JSON.stringify({
          txid: coin.spentTxid,
          category: 'send',
          satoshis: -coin.value,
          height: coin.spentHeight,
          address: coin.address,
          outputIndex: coin.mintIndex
        }) + '\n'
      );
    }

    if (coin.mintTxid) {
      this.push(
        JSON.stringify({
          txid: coin.mintTxid,
          category: 'receive',
          satoshis: coin.value,
          height: coin.mintHeight,
          address: coin.address,
          outputIndex: coin.mintIndex
        }) + '\n'
      );
    }
  }

  async _transform(coin, _, done) {
    await this.writeTxToStream(coin);
    done();
  }
}
