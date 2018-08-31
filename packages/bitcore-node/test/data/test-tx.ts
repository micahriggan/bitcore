export const TEST_TX = {
  hash: '08e23107e8449f02568d37d37aa76e840e55bbb5f100ed8ad257af303db88c08',
  _hash: '08e23107e8449f02568d37d37aa76e840e55bbb5f100ed8ad257af303db88c08',
  isCoinbase: () => true,
  outputAmount: 0.09765625,
  inputs: [],
  outputs: [
    {
      account: '',
      address: 'mjN5cBHegwmVZ61RKnCdsBG53QGBS2LpBo',
      category: 'generate',
      satoshis: 0.09765625,
      vout: 0,
      script: {
        toBuffer: () => Buffer.from(''),
        classify: () => '',
        chunks: new Array<{ buf: Buffer }>(),
        toAddress: () => ''
      }
    }
  ],
  nLockTime: 0,
  toBuffer: () => Buffer.from('')
};
export const TEST_TX_1 = {
  hash: 'b8abbdd4428b32cdf79a29728ea7a6d102444c880dca9be489c1ba346dcc5436',
  _hash: 'b8abbdd4428b32cdf79a29728ea7a6d102444c880dca9be489c1ba346dcc5436',
  isCoinbase: () => false,
  outputAmount: 0.0976,
  inputs: [
    {
      toObject: () => {
        return {
          prevTxId: '08e23107e8449f02568d37d37aa76e840e55bbb5f100ed8ad257af303db88c08',
          outputIndex: 0
        };
      }
    }
  ],
  outputs: [
    {
      account: '',
      address: 'mjN5cBHegwmVZ61RKnCdsBG53QGBS2LpBo',
      category: 'generate',
      satoshis: 0.09765625,
      vout: 0,
      script: {
        toBuffer: () => Buffer.from(''),
        classify: () => '',
        chunks: new Array<{ buf: Buffer }>(),
        toAddress: () => ''
      }
    }
  ],
  nLockTime: 0,
  toBuffer: () => Buffer.from('')
};
export const TEST_TX_2 = {
  hash: '1e28aa7b910f256dd49f020a668b69c427c2646bfc99b4f892deea71bb885062',
  _hash: '1e28aa7b910f256dd49f020a668b69c427c2646bfc99b4f892deea71bb885062',
  isCoinbase: () => true,
  outputAmount: 0.06763325,
  inputs: [],
  outputs: [],
  nLockTime: 0,
  toBuffer: () => Buffer.from('')
};
export const TEST_TX_3 = {
  hash: '947911ecc53cd8313220c94ba2211b90a4062a79ee8f830b100861c377f501ef',
  _hash: '947911ecc53cd8313220c94ba2211b90a4062a79ee8f830b100861c377f501ef',
  isCoinbase: () => true,
  outputAmount: 0.07865625,
  inputs: [],
  outputs: [],
  nLockTime: 0,
  toBuffer: () => Buffer.from('')
};
