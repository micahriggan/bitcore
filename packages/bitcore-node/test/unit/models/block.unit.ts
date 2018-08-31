import { expect } from 'chai';
import { BlockModel, IBlock } from '../../../src/models/block';
import { TransactionModel } from '../../../src/models/transaction';
import { CoinModel } from '../../../src/models/coin';
import * as sinon from 'sinon';
import { TEST_BLOCK } from '../../data/test-block';
import { Storage } from '../../../src/services/storage';
import { mockStorage } from '../../helpers';
import { mockCollection } from '../../helpers/index.js';
import { ChainStateProvider } from '../../../src/providers/chain-state';
import { Adapters, Bucket } from '../../../src/adapters';
import { Bitcoin } from '../../../src/types/namespaces/Bitcoin';
import { TEST_TX_1 } from '../../data/test-tx';
import { MongoBound } from '../../../src/models/base';

describe('Block Model', function() {
  describe('addBlock', () => {
    const convertedBlock = Adapters.convertBlock<Bitcoin.Block>({
      chain: 'BTC',
      network: 'testnet',
      block: TEST_BLOCK
    });
    let addBlockParams = {
      chain: 'BTC',
      network: 'regtest',
      block: convertedBlock,
      transactions: [],
      height: 1355,
      initialSyncComplete: false
    };
    let sandbox;
    beforeEach(() => {
      sandbox = sinon.sandbox.create();
    });
    afterEach(() => {
      sandbox.restore();
    });
    it('should be able to add a block', async () => {
      let newBlock = Object.assign({ save: () => Promise.resolve() }, BlockModel, addBlockParams);

      mockStorage(newBlock);
      sandbox.stub(BlockModel, 'handleReorg').resolves();
      sandbox.stub(TransactionModel, 'batchImport').resolves();

      const result = (await BlockModel.addBlock(addBlockParams)).result;
      expect(addBlockParams.block.hash).to.be.equal(result.block.hash);
      expect(addBlockParams.height).to.be.equal(result.height);
      expect(addBlockParams.chain).to.be.equal(result.chain);
    });
  });

  describe('getLocalTip', () => {
    let sandbox;
    beforeEach(() => {
      sandbox = sinon.sandbox.create();
    });
    afterEach(() => {
      sandbox.restore();
    });
    it('should return null if there are no blocks', async () => {
      mockStorage(null);
      const params = { chain: 'BTC', network: 'regtest' };
      const result = await ChainStateProvider.getLocalTip(params);
      expect(result).to.deep.equal(null);
    });
  });

  describe('getPoolInfo', () => {
    xit('should return pool info given a coinbase string');
  });

  describe('getLocatorHashes', () => {
    let sandbox;
    beforeEach(() => {
      sandbox = sinon.sandbox.create();
    });
    afterEach(() => {
      sandbox.restore();
    });
    it('should return 65 zeros if there are no processed blocks for the chain and network', async () => {
      const params = { chain: 'BTC', network: 'regtest' };
      const result = await ChainStateProvider.getLocatorHashes(params);
      expect(result).to.deep.equal([Array(65).join('0')]);
    });
  });

  describe('handleReorg', () => {
    let sandbox;
    beforeEach(() => {
      sandbox = sinon.sandbox.create();
    });
    afterEach(() => {
      sandbox.restore();
    });

    it('should return if localTip hash equals the previous hash', async () => {
      Object.assign(BlockModel.collection, mockCollection(null));
      Object.assign(TransactionModel.collection, mockCollection(null));
      Object.assign(CoinModel.collection, mockCollection(null));
      let blockModelRemoveSpy = BlockModel.collection.remove as sinon.SinonSpy;
      let transactionModelRemoveSpy = TransactionModel.collection.remove as sinon.SinonSpy;
      let coinModelRemoveSpy = CoinModel.collection.remove as sinon.SinonSpy;
      let coinModelUpdateSpy = CoinModel.collection.update as sinon.SinonSpy;

      const params = {
        header: {
          prevHash: '3420349f63d96f257d56dd970f6b9079af9cf2784c267a13b1ac339d47031fe9',
          hash: '64bfb3eda276ae4ae5b64d9e36c9c0b629bc767fb7ae66f9d55d2c5c8103a929',
          time: 1526756523,
          version: '536870912',
          merkleRoot: '08e23107e8449f02568d37d37aa76e840e55bbb5f100ed8ad257af303db88c08',
          bits: parseInt('207fffff', 16).toString(),
          nonce: '2'
        },
        chain: 'BTC',
        network: 'regtest'
      };

      await BlockModel.handleReorg(params);
      expect(blockModelRemoveSpy.notCalled).to.be.true;
      expect(transactionModelRemoveSpy.notCalled).to.be.true;
      expect(coinModelRemoveSpy.notCalled).to.be.true;
      expect(coinModelUpdateSpy.notCalled).to.be.true;
    });

    it('should return if localTip height is zero', async () => {
      let blockModelRemoveSpy = BlockModel.collection.remove as sinon.SinonSpy;
      let transactionModelRemoveSpy = TransactionModel.collection.remove as sinon.SinonSpy;
      let coinModelRemoveSpy = CoinModel.collection.remove as sinon.SinonSpy;
      let coinModelUpdateSpy = CoinModel.collection.update as sinon.SinonSpy;

      let blockMethodParams = {
        chain: 'BTC',
        network: 'regtest',
        block: TEST_BLOCK,
        height: 1355
      };
      let params = Object.assign(BlockModel, blockMethodParams);

      await BlockModel.handleReorg(params);
      expect(blockModelRemoveSpy.notCalled).to.be.true;
      expect(transactionModelRemoveSpy.notCalled).to.be.true;
      expect(coinModelRemoveSpy.notCalled).to.be.true;
      expect(coinModelUpdateSpy.notCalled).to.be.true;
    });

    it('should call blockModel remove', async () => {
      mockStorage({
        height: 1,
        previousBlockHash: '3420349f63d96f257d56dd970f6b9079af9cf2784c267a13b1ac339d47031fe9'
      });
      let blockMethodParams = {
        chain: 'BTC',
        network: 'regtest',
        block: TEST_BLOCK,
        height: 1355
      };
      let params = Object.assign(BlockModel, blockMethodParams);
      const removeSpy = BlockModel.collection.remove as sinon.SinonSpy;

      await BlockModel.handleReorg(params);
      expect(removeSpy.called).to.be.true;
    });

    it('should call transactionModel remove', async () => {
      mockStorage({
        height: 1,
        previousBlockHash: '3420349f63d96f257d56dd970f6b9079af9cf2784c267a13b1ac339d47031fe9'
      });

      let blockMethodParams = {
        chain: 'BTC',
        network: 'regtest',
        block: TEST_BLOCK,
        height: 1355
      };
      let params = Object.assign(BlockModel, blockMethodParams);
      const removeSpy = TransactionModel.collection.remove as sinon.SinonSpy;

      await BlockModel.handleReorg(params);
      expect(removeSpy.called).to.be.true;
    });

    it('should call coinModel remove', async () => {
      mockStorage({
        height: 1,
        previousBlockHash: '3420349f63d96f257d56dd970f6b9079af9cf2784c267a13b1ac339d47031fe9'
      });

      let blockMethodParams = {
        chain: 'BTC',
        network: 'regtest',
        block: TEST_BLOCK,
        height: 1355
      };
      let params = Object.assign(BlockModel, blockMethodParams);
      const collectionSpy = Storage.db!.collection as sinon.SinonSpy;
      const removeSpy = CoinModel.collection.remove as sinon.SinonSpy;

      await BlockModel.handleReorg(params);
      expect(collectionSpy.calledOnceWith('coins'));
      expect(removeSpy.callCount).to.eq(3);
    });

    it('should call coinModel update', async () => {
      mockStorage({
        height: 1,
        previousBlockHash: '3420349f63d96f257d56dd970f6b9079af9cf2784c267a13b1ac339d47031fe9'
      });

      let blockMethodParams = {
        chain: 'BTC',
        network: 'regtest',
        block: TEST_BLOCK,
        height: 1355
      };
      let params = Object.assign(BlockModel, blockMethodParams);
      const collectionSpy = Storage.db!.collection as sinon.SinonSpy;
      const updateSpy = CoinModel.collection.update as sinon.SinonSpy;

      await BlockModel.handleReorg(params);
      expect(collectionSpy.calledOnceWith('coins'));
      expect(updateSpy.called).to.be.true;
    });
  });

  describe('_apiTransform', () => {
    it('should return the transform object with block values', () => {
      const block: IBlock = {
        chain: 'BTC',
        network: 'mainnet',
        height: 1,
        hash: 'abcd',
        version: 1,
        merkleRoot: 'deff',
        time: new Date(),
        timeNormalized: new Date(),
        nonce: 1,
        previousBlockHash: 'aabb',
        nextBlockHash: 'bbcc',
        transactionCount: 1,
        size: 255,
        bits: 256,
        reward: 5000000000,
        processed: true
      };

      const result = BlockModel._apiTransform(block, { object: true });

      expect(result.hash).to.be.equal(block.hash);
      expect(result.height).to.be.equal(block.height);
      expect(result.version).to.be.equal(block.version);
      expect(result.size).to.be.equal(block.size);
      expect(result.merkleRoot).to.be.equal(block.merkleRoot);
      expect(result.time).to.equal(block.time);
      expect(result.timeNormalized).to.equal(block.timeNormalized);
      expect(result.nonce).to.be.equal(block.nonce);
      expect(result.bits).to.be.equal(block.bits);
      expect(result.previousBlockHash).to.be.equal(block.previousBlockHash);
      expect(result.nextBlockHash).to.be.equal(block.nextBlockHash);
      expect(result.transactionCount).to.be.equal(block.transactionCount);
      expect(result).to.not.have.property('processed');
    });
  });

  it('should be able to parse an IBlock and generate mongo operations', async () => {
    const convertedBlock = Adapters.convertBlock({ chain: 'BTC', network: 'mainnet', block: TEST_BLOCK });
    const convertedTransactions = TEST_BLOCK.transactions.map(t =>
      Adapters.convertTx({ chain: 'BTC', network: 'mainnet', tx: t, block: convertedBlock })
    );
    mockStorage({});
    const blockOperation = await BlockModel.getBlockOp({
      block: convertedBlock,
      transactions: convertedTransactions,
      initialSyncComplete: false,
      chain: 'BTC',
      network: 'mainnet'
    });

    expect(blockOperation.blockOp.$set.chain).to.be.eq('BTC');
    expect(blockOperation.blockOp.$set.hash).to.be.eq(TEST_BLOCK.hash);
    expect(blockOperation.blockOp.$set.merkleRoot).to.be.eq(convertedBlock.merkleRoot);
    expect(blockOperation.blockOp.$set.bits).to.be.eq(convertedBlock.bits);
    expect(blockOperation.blockOp.$set.network).to.be.eq('mainnet');
    expect(blockOperation.blockOp.$set.processed).to.be.eq(false);
    expect(blockOperation.blockOp.$set.reward).to.be.eq(convertedTransactions[0].outputs[0].value);
    expect(blockOperation.blockOp.$set.transactionCount).to.be.eq(TEST_BLOCK.transactions.length);

    const outputCount = convertedTransactions.reduce((sum, tx) => sum + tx.outputs.length, 0);
    expect(blockOperation.mintOps.length).to.be.eq(outputCount);
    expect(blockOperation.mintOps.length).to.be.gt(0);
    expect(blockOperation.mintOps[0].updateOne.update.$set.address).to.be.eq(
      convertedTransactions[0].outputs[0].address
    );
    expect(blockOperation.mintOps[0].updateOne.update.$set.value).to.be.eq(convertedTransactions[0].outputs[0].value);

    expect(blockOperation.spendOps.length).to.be.eq(0);

    expect(blockOperation.txOps.length).to.be.eq(1);
    expect(blockOperation.txOps[0].updateOne.update.$set.blockHash).to.be.eq(convertedBlock.hash);
    // since no previous blocks, start at block height 1
    expect(blockOperation.txOps[0].updateOne.update.$set.blockHeight).to.be.eq(1);
    expect(blockOperation.txOps[0].updateOne.update.$set.blockHash).to.be.eq(convertedBlock.hash);
    expect(blockOperation.txOps[0].updateOne.update.$set.txid).to.be.eq(convertedTransactions[0].txid);
  });

  it('should be able to handle intra block spends', async () => {
    const intraSpendBlock = Object.assign({}, TEST_BLOCK);
    intraSpendBlock.transactions.push(TEST_TX_1);
    const convertedBlock = Adapters.convertBlock({ chain: 'BTC', network: 'mainnet', block: intraSpendBlock});
    const convertedTransactions = intraSpendBlock.transactions.map(t =>
      Adapters.convertTx({ chain: 'BTC', network: 'mainnet', tx: t, block: convertedBlock })
    );
    mockStorage({});
    const blockOperation = await BlockModel.getBlockOp({
      block: convertedBlock,
      transactions: convertedTransactions,
      initialSyncComplete: false,
      chain: 'BTC',
      network: 'mainnet'
    });

    expect(blockOperation.mintOps[0].updateOne.update.$set.spentTxid).to.be.eq(TEST_TX_1.hash);
    expect(blockOperation.mintOps[0].updateOne.update.$set.spentHeight).to.be.eq(1);
  });

  it('should be able to handle intra batch spends', async () => {
    const convertedBlock = Adapters.convertBlock({ chain: 'BTC', network: 'mainnet', block: TEST_BLOCK});
    const convertedTransactions = TEST_BLOCK.transactions.map(t =>
      Adapters.convertTx({ chain: 'BTC', network: 'mainnet', tx: t, block: convertedBlock })
    );
    mockStorage({});
    const blockOperation = await BlockModel.getBlockOp({
      block: convertedBlock,
      transactions: convertedTransactions,
      initialSyncComplete: false,
      chain: 'BTC',
      network: 'mainnet'
    });

    const intraBatchSpendBlock = Object.assign({}, TEST_BLOCK);
    intraBatchSpendBlock.transactions.push(TEST_TX_1);
    const convertedBlock2 = Adapters.convertBlock({ chain: 'BTC', network: 'mainnet', block: intraBatchSpendBlock});
    const convertedTransactions2 = intraBatchSpendBlock.transactions.map(t =>
      Adapters.convertTx({ chain: 'BTC', network: 'mainnet', tx: t, block: convertedBlock })
    );

    const blockOperation2 = await BlockModel.getBlockOp({
      block: convertedBlock2,
      transactions: convertedTransactions2,
      initialSyncComplete: false,
      mintOps: blockOperation.mintOps,
      previousBlock: convertedBlock as Bucket<MongoBound<IBlock>>,
      chain: 'BTC',
      network: 'mainnet'
    });

    expect(blockOperation.mintOps[0].updateOne.update.$set.spentTxid).to.be.eq(TEST_TX_1.hash);
    expect(blockOperation.mintOps[0].updateOne.update.$set.spentHeight).to.be.eq(1);
    expect(blockOperation2.blockOp.$set.height).to.be.eq(2);
  });
});
