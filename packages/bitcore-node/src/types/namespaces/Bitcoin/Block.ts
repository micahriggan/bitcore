import { BitcoinTransactionType } from "./Transaction";
export type BlockHeaderObj = {
  prevHash: string;
  hash: string;
  time: number;
  version: string;
  merkleRoot: string;
  bits: string;
  nonce: string;
  tx: string[];
}
export type BlockHeader = {
  toObject: () => BlockHeaderObj;
};
export type BitcoinBlockType = {
  hash: string;
  transactions: BitcoinTransactionType[];
  header: BlockHeader;
  toBuffer: () => Buffer;
};
