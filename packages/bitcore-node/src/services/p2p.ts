import config from '../config';
import logger from '../logger';
import { EventEmitter } from 'events';
import { BlockModel, IBlock, BlockOp } from '../models/block';
import { ChainStateProvider } from '../providers/chain-state';
import { TransactionModel, TxOp, CoinSpendOp, CoinMintOp } from '../models/transaction';
import { Bitcoin } from '../types/namespaces/Bitcoin';
import { StateModel } from '../models/state';
import { Adapters, VerboseTransaction, Bucket } from '../adapters';
import { Chain } from '../chain';
import { LoggifyClass } from "../decorators/Loggify";
const LRU = require('lru-cache');

@LoggifyClass
export class P2pService {
  private chain: string;
  private network: string;
  private bitcoreLib: any;
  private bitcoreP2p: any;
  private chainConfig: any;
  private events: EventEmitter;
  private syncing: boolean;
  private messages: any;
  private pool: any;
  private invCache: any;
  private initialSyncComplete: boolean;
  constructor(params) {
    const { chain, network, chainConfig } = params;
    this.chain = chain;
    this.network = network;
    this.bitcoreLib = Chain[this.chain].lib;
    this.bitcoreP2p = Chain[this.chain].p2p;
    this.chainConfig = chainConfig;
    this.events = new EventEmitter();
    this.syncing = false;
    this.initialSyncComplete = false;
    this.invCache = new LRU({ max: 10000 });
    this.messages = new this.bitcoreP2p.Messages({
      network: this.bitcoreLib.Networks.get(this.network)
    });
    this.pool = new this.bitcoreP2p.Pool({
      addrs: this.chainConfig.trustedPeers.map(peer => {
        return {
          ip: {
            v4: peer.host
          },
          port: peer.port
        };
      }),
      dnsSeed: false,
      listenAddr: false,
      network: this.network,
      messages: this.messages
    });
  }

  setupListeners() {
    this.pool.on('peerready', peer => {
      logger.info(`Connected to peer ${peer.host}`, {
        chain: this.chain,
        network: this.network
      });
    });

    this.pool.on('peerdisconnect', peer => {
      logger.warn(`Not connected to peer ${peer.host}`, {
        chain: this.chain,
        network: this.network,
        port: peer.port
      });
    });

    this.pool.on('peertx', (peer, message) => {
      const hash = message.transaction.hash;
      logger.debug('peer tx received', {
        peer: `${peer.host}:${peer.port}`,
        chain: this.chain,
        network: this.network,
        hash
      });
      if (!this.invCache.get(hash)) {
        this.processTransaction(message.transaction);
        this.events.emit('transaction', message.transaction);
      }
      this.invCache.set(hash);
    });

    this.pool.on('peerblock', async (peer, message) => {
      const { block } = message;
      const { hash } = block;
      const { chain, network } = this;
      logger.debug('peer block received', {
        peer: `${peer.host}:${peer.port}`,
        chain: this.chain,
        network: this.network,
        hash
      });

      if (!this.invCache.get(hash)) {
        this.invCache.set(hash);
        this.events.emit(hash, message.block);
        if (!this.syncing) {
          try {
            await this.processBlock(block);
            this.events.emit('block', message.block);
          } catch (err) {
            logger.error(`Error syncing ${chain} ${network}`, err);
            await this.sync();
          }
        }
      }
    });

    this.pool.on('peerheaders', (peer, message) => {
      logger.debug('peerheaders message received', {
        peer: `${peer.host}:${peer.port}`,
        chain: this.chain,
        network: this.network,
        count: message.headers.length
      });
      this.events.emit('headers', message.headers);
    });

    this.pool.on('peerinv', (peer, message) => {
      if (!this.syncing) {
        const filtered = message.inventory.filter(inv => {
          const hash = this.bitcoreLib.encoding
            .BufferReader(inv.hash)
            .readReverse()
            .toString('hex');
          return !this.invCache.get(hash);
        });

        if (filtered.length) {
          peer.sendMessage(this.messages.GetData(filtered));
        }
      }
    });
  }

  async connect() {
    this.pool.connect();
    setInterval(this.pool.connect.bind(this.pool), 5000);
    return new Promise<void>(resolve => {
      this.pool.once('peerready', () => resolve());
    });
  }

  static startConfiguredChains() {
    for (let chain of Object.keys(config.chains)) {
      for (let network of Object.keys(config.chains[chain])) {
        const chainConfig = config.chains[chain][network];
        if (chainConfig.chainSource && chainConfig.chainSource !== 'p2p') {
          continue;
        }
        if (!Object.keys(Chain).includes(chain)) {
          continue;
        }
        new P2pService({
          chain,
          network,
          chainConfig
        }).start();
      }
    }
  }

  public async getHeaders(candidateHashes: string[]) {
    return new Promise(resolve => {
      const _getHeaders = () => {
        this.pool.sendMessage(
          this.messages.GetHeaders({
            starts: candidateHashes
          })
        );
      };
      const headersRetry = setInterval(_getHeaders, 1000);

      this.events.once('headers', headers => {
        clearInterval(headersRetry);
        resolve(headers);
      });
      _getHeaders();
    });
  }

  public async getBlock(hash: string): Promise<Bitcoin.Block> {
    return new Promise<Bitcoin.Block>(resolve => {
      logger.debug('Getting block, hash:', hash);
      const _getBlock = () => {
        this.pool.sendMessage(this.messages.GetData.forBlock(hash));
      };
      const getBlockRetry = setInterval(_getBlock, 1000);

      this.events.once(hash, block => {
        logger.debug('Received block, hash:', hash);
        clearInterval(getBlockRetry);
        resolve(block);
      });
      _getBlock();
    });
  }

  getBestPoolHeight(): number {
    let best = 0;
    for (const peer of Object.values(this.pool._connectedPeers) as { bestHeight: number }[]) {
      if (peer.bestHeight > best) {
        best = peer.bestHeight;
      }
    }
    return best;
  }

  async getBlockOperations(
    block: Bucket<IBlock>,
    transactions: VerboseTransaction[],
    mintOps,
    spendOps,
    txOps,
    previousBlock
  ) {
    return BlockModel.getBlockOp({
      chain: this.chain,
      network: this.network,
      forkHeight: this.chainConfig.forkHeight,
      parentChain: this.chainConfig.parentChain,
      initialSyncComplete: this.initialSyncComplete,
      block,
      transactions,
      mintOps,
      spendOps,
      txOps,
      previousBlock
    });
  }

  getConvertedBlock(block: Bitcoin.Block) {
    const convertedBlock = Adapters.convertBlock<Bitcoin.Block>({
      chain: this.chain,
      network: this.network,
      block
    });
    const convertedTransactions = block.transactions.map(t =>
      Adapters.convertTx<Bitcoin.Transaction>({
        chain: this.chain,
        network: this.network,
        tx: t,
        block: convertedBlock
      })
    );
    return { convertedBlock, convertedTransactions };
  }

  async processBlock(block: Bitcoin.Block): Promise<any> {
    return new Promise(async (resolve, reject) => {
      try {
        const { convertedBlock, convertedTransactions } = this.getConvertedBlock(block);
        await BlockModel.addBlock({
          chain: this.chain,
          network: this.network,
          forkHeight: this.chainConfig.forkHeight,
          parentChain: this.chainConfig.parentChain,
          initialSyncComplete: this.initialSyncComplete,
          block: convertedBlock,
          transactions: convertedTransactions
        });
        if (!this.syncing) {
          logger.info(`Added block ${block.hash}`, {
            chain: this.chain,
            network: this.network
          });
        }
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }

  async processTransaction(tx: Bitcoin.Transaction): Promise<any> {
    const now = new Date();
    const convertedTransactions = Adapters.convertTx<Bitcoin.Transaction>({
      chain: this.chain,
      network: this.network,
      tx
    });
    TransactionModel.batchImport({
      chain: this.chain,
      network: this.network,
      txs: [convertedTransactions],
      height: -1,
      mempoolTime: now,
      blockTime: now,
      blockTimeNormalized: now,
      initialSyncComplete: true
    });
  }

  async sync() {
    if (this.syncing) {
      return;
    }
    this.syncing = true;
    const { chain, chainConfig, network } = this;
    const { parentChain, forkHeight } = chainConfig;
    const state = await StateModel.collection.findOne({});
    this.initialSyncComplete =
      state && state.initialSyncComplete && state.initialSyncComplete.includes(`${chain}:${network}`);
    let tip = await ChainStateProvider.getLocalTip({ chain, network });
    if (parentChain && (!tip || tip.height < forkHeight)) {
      let parentTip = await ChainStateProvider.getLocalTip({ chain: parentChain, network });
      while (!parentTip || parentTip.height < forkHeight) {
        logger.info(`Waiting until ${parentChain} syncs before ${chain} ${network}`);
        await new Promise(resolve => {
          setTimeout(resolve, 5000);
        });
        parentTip = await ChainStateProvider.getLocalTip({ chain: parentChain, network });
      }
    }

    const getHeaders = async () => {
      const locators = await ChainStateProvider.getLocatorHashes({ chain, network });
      return this.getHeaders(locators);
    };

    let headers;
    let blockBatch = new Array<BlockOp>();
    let mintBatch = new Array<CoinMintOp>();
    let spendBatch = new Array<CoinSpendOp>();
    let txBatch = new Array<TxOp>();
    let prevBlock: IBlock | null = null;
    while (!headers || headers.length > 0) {
      headers = await getHeaders();
      tip = await ChainStateProvider.getLocalTip({ chain, network });
      let prevPromise: Promise<any> | null = null;
      let currentHeight = tip ? tip.height : 0;
      let lastLog = 0;
      logger.info(`Syncing ${headers.length} blocks for ${chain} ${network}`);
      for (const header of headers) {
        try {
          const block = await this.getBlock(header.hash);
          const { convertedBlock, convertedTransactions } = this.getConvertedBlock(block);

          // Get back a group of mongo operations for this block
          const blockUpdates = await this.getBlockOperations(
            convertedBlock,
            convertedTransactions,
            mintBatch,
            spendBatch,
            txBatch,
            prevBlock
          );

          // keep a running batch to help with reducing spends within a batch
          blockBatch = blockBatch.concat(blockUpdates);
          mintBatch = mintBatch.concat(blockUpdates.mintOps);
          spendBatch = spendBatch.concat(blockUpdates.spendOps);
          txBatch = txBatch.concat(blockUpdates.txOps);
          prevBlock = blockUpdates.blockOp.$set;
          if (Date.now() - lastLog > 100) {
            logger.info(`Sync `, {
              chain,
              network,
              height: currentHeight,
              mintQueue: mintBatch.length
            });
            lastLog = Date.now();
          }

          if (blockBatch.length > 500 || mintBatch.length > 150000) {
            logger.info(`Writing ${blockBatch.length} blocks `, {
              chain,
              network,
              height: currentHeight
            });
            if (prevPromise === null) {
              prevPromise = BlockModel.processBlockOps(blockBatch);
            } else {
              await prevPromise;
              prevPromise = BlockModel.processBlockOps(blockBatch);
            }
            blockBatch = new Array<BlockOp>();
            mintBatch = new Array<CoinMintOp>();
            spendBatch = new Array<CoinSpendOp>();
            txBatch = new Array<TxOp>();
          }

          currentHeight++;
        } catch (err) {
          logger.error(`Error syncing ${chain} ${network}`, err);
          this.syncing = false;
          return this.sync();
        }
      }
      if (mintBatch.length > 0) {
        // clear out the remaining operations before trying to get headers
        logger.info(`Writing ${blockBatch.length} blocks `, {
          chain,
          network,
          height: currentHeight
        });
        if (prevPromise) await prevPromise;
        await BlockModel.processBlockOps(blockBatch);
        blockBatch = new Array<BlockOp>();
        mintBatch = new Array<CoinMintOp>();
        spendBatch = new Array<CoinSpendOp>();
        txBatch = new Array<TxOp>();
      }
    }
    logger.info(`${chain}:${network} up to date.`);
    this.syncing = false;
    StateModel.collection.findOneAndUpdate(
      {},
      { $addToSet: { initialSyncComplete: `${chain}:${network}` } },
      { upsert: true }
    );
    return true;
  }

  async start() {
    logger.debug(`Started worker for chain ${this.chain}`);
    this.setupListeners();
    await this.connect();
    this.sync();
  }
}
