export declare namespace Ethereum {
  export type Block = {
    parentHash: string;
    uncleHash: string;
    coinbase: string;
    stateRoot: string;
    transactionsTrie: string;
    receiptTrie: string;
    bloom: string;
    difficulty: string;
    number: number;
    gasLimit: number;
    gasUsed: number;
    timestamp: number;
    extraData: string;
    mixHash: string;
    nonce: number;
    raw: Array<Buffer>;
  };

  export type Transaction = {
    hash: string;
    nonce: string;
    gasPrice: string;
    gasLimit: string;
    to: string;
    value: number;
    data: string;
    // EIP 155 chainId - mainnet: 1, ropsten: 3
    chainId: number;
  };
}
