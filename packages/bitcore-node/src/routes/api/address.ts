import express = require('express');
const router = express.Router({ mergeParams: true });
import { ChainStateProvider } from '../../providers/chain-state';
import { CoinStorage, ICoin } from '../../models/coin';
import { Storage } from '../../services/storage';
import { Transform } from 'stream';

router.get('/:address/txs', function(req, res) {
  let { address, chain, network } = req.params;
  let { unspent, limit = 10, since } = req.query;
  let payload = {
    chain,
    network,
    address,
    req,
    res,
    args: { ...req.query, unspent, limit, since }
  };
  ChainStateProvider.streamAddressTransactions(payload);
});

router.get('/:address', function(req, res) {
  let { address, chain, network } = req.params;
  let { unspent, limit = 10, since } = req.query;
  let payload = {
    chain,
    network,
    address,
    req,
    res,
    args: { unspent, limit, since }
  };
  ChainStateProvider.streamAddressUtxos(payload);
});

router.get('/:address/coins', async function(req, res) {
  let { address, chain, network } = req.params;
  const { limit = 10, since } = req.query;
  const args = { ...req.query, limit, since };
  try {
    const query = { address, chain, network };
    const coinStream = Storage.streamingFind(CoinStorage, query, args, c =>
      CoinStorage._apiTransform(c, { object: true })
    ).pipe(
      new Transform({
        objectMode: true,
        transform: async function(data, _, done) {
          const coin: ICoin = (data as any) as ICoin;
          const [outputs, inputs] = await Promise.all([
            CoinStorage.collection.find({ chain, network, mintTxid: coin.mintTxid }).toArray(),
            CoinStorage.collection.find({ chain, network, spentTxid: coin.mintTxid }).toArray()
          ]);
          const payload = {
            coin: CoinStorage._apiTransform(coin, { object: true }),
            inputs: inputs.map(i => CoinStorage._apiTransform(i, { object: true })),
            outputs: outputs.map(o => CoinStorage._apiTransform(o, { object: true }))
          };
          done(null, payload);
        }
      })
    );
    return Storage.stream(coinStream, req, res);
  } catch (err) {
    return res.status(500).send(err);
  }
});

router.get('/:address/balance', async function(req, res) {
  let { address, chain, network } = req.params;
  try {
    let result = await ChainStateProvider.getBalanceForAddress({
      chain,
      network,
      address,
      args: req.query
    });
    return res.send(result || { confirmed: 0, unconfirmed: 0, balance: 0 });
  } catch (err) {
    return res.status(500).send(err);
  }
});

module.exports = {
  router: router,
  path: '/address'
};
