import { BitcoinAdapter } from '../../../src/adapters/bitcoin';
import { TEST_BLOCK } from '../../data/test-block';
import { expect } from 'chai';

describe('Bitcoin Adapter', () => {
  const adapter = new BitcoinAdapter();
  it('should be able to convert a bitcoin block to IBlock', () => {
    const convertedBlock = adapter.convertBlock({ chain: 'BTC', network: 'mainnet', block: TEST_BLOCK });
    expect(convertedBlock.hash).to.be.eq(TEST_BLOCK.hash);
    expect(convertedBlock.chain).to.be.eq('BTC');
    expect(convertedBlock.network).to.be.eq('mainnet');
    expect(convertedBlock.height).to.eq(-1);
    expect(convertedBlock.transactionCount).to.be.eq(TEST_BLOCK.transactions.length);
    expect(convertedBlock.reward).to.be.eq(TEST_BLOCK.transactions[0].outputAmount);
  });
});
