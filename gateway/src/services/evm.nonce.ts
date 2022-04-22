import ethers from 'ethers';
import { logger } from './logger';
import { LocalStorage } from './local-storage';
import {
  InitializationError,
  SERVICE_UNITIALIZED_ERROR_CODE,
  SERVICE_UNITIALIZED_ERROR_MESSAGE,
} from './error-handler';

export class NonceLocalStorage extends LocalStorage {
  public async saveNonce(
    chain: string,
    chainId: number,
    address: string,
    nonce: number
  ): Promise<void> {
    return this.save(chain + '/' + String(chainId) + '/' + address, nonce);
  }

  public async deleteNonce(
    chain: string,
    chainId: number,
    address: string
  ): Promise<void> {
    return this.del(chain + '/' + String(chainId) + '/' + address);
  }

  public async getNonces(
    chain: string,
    chainId: number
  ): Promise<Record<string, number>> {
    return this.get((address: string, nonce: any) => {
      const splitKey: string[] = address.split('/');
      if (
        splitKey.length === 3 &&
        splitKey[0] === chain &&
        splitKey[1] === String(chainId)
      ) {
        return [splitKey[2], parseInt(nonce)];
      } else if (
        splitKey.length === 4 &&
        splitKey[0] === chain &&
        splitKey[1] === String(chainId) &&
        splitKey[3] === String('leading')
      ) {
        return [`${splitKey[2]}/${splitKey[3]}`, parseInt(nonce)];
      }
      return;
    });
  }
}

/**
 * Manages EVM nonce for addresses to ensure logical consistency of nonces when
 * there is a burst of transactions being sent out.
 *
 * This class aims to solve the following problems:
 *
 * 1. Sending multiple EVM transactions concurrently.
 *    Naively, developers would use the transaction count from the EVM node as
 *    the nonce for new transactions. When multiple transactions are being sent
 *    out this way - these transactions would often end up using the same nonce
 *    and thus only one of them would succeed.
 *    The EVM nonce manager ensures the correct serialization of nonces used in
 *    this case, s.t. the nonces for new concurrent transactions will go out as
 *    [n, n+1, n+2, ...] rathan than [n, n, n, ...]
 *
 * 2. Stuck or dropped transactions.
 *    If you've sent out a transaction with nonce n before, but it got stuck or
 *    was dropped from the mem-pool - it's better to just forget about its nonce
 *    and send the next transaction with its nonce rather than to wait for it to
 *    be confirmed.
 *    This is where the `localNonceTTL` parameter comes in. The locally cached
 *    nonces are only remembered for a period of time (default is 5 minutes).
 *    After that, nonce values from the EVM node will be used again to prevent
 *    potentially dropped nonces from blocking new transactions.
 *
 * 3. Canceling, or re-sending past transactions.
 *    Canceling or re-sending past transactions would typically re-use past
 *    nonces. This means the user is intending to reset his transaction chain
 *    back to a certain nonce. The manager should allow the cached nonce to go
 *    back to the specified past nonce when it happens.
 *    This means whenever a transaction is sent with a past nonce or an EVM
 *    cancel happens, the API logic **must** call commitNonce() to reset the
 *    cached nonce back to the specified position.
 */
export class EVMNonceManager {
  #addressToNonce: Record<string, [number, Date]> = {};
  #addressToLeadingNonce: Record<string, [number, Date]> = {};

  #initialized: boolean = false;
  #chainId: number;
  #chainName: string;
  #localNonceTTL: number;
  #db: NonceLocalStorage;

  // this should be private but then we cannot mock it
  public _provider: ethers.providers.Provider | null = null;

  constructor(
    chainName: string,
    chainId: number,
    localNonceTTL: number = 300,
    dbPath: string = 'gateway.level'
  ) {
    this.#chainName = chainName;
    this.#chainId = chainId;
    this.#localNonceTTL = localNonceTTL;
    this.#db = new NonceLocalStorage(dbPath);
  }

  // init can be called many times and generally should always be called
  // getInstance, but it only applies the values the first time it is called
  public async init(provider: ethers.providers.Provider): Promise<void> {
    if (this.#localNonceTTL < 0) {
      throw new InitializationError(
        SERVICE_UNITIALIZED_ERROR_MESSAGE(
          'EVMNonceManager.init delay must be greater than or equal to zero.'
        ),
        SERVICE_UNITIALIZED_ERROR_CODE
      );
    }

    if (!this._provider) {
      this._provider = provider;
    }

    if (!this.#initialized) {
      const addressToNonce: Record<string, number> = await this.#db.getNonces(
        this.#chainName,
        this.#chainId
      );

      for (const [address, nonce] of Object.entries(addressToNonce)) {
        logger.info(address + ':' + String(nonce));
        if (address.includes('/leading')) {
          const splitKey: string[] = address.split('/');
          this.#addressToLeadingNonce[splitKey[0]] = [nonce, new Date()];
        } else {
          this.#addressToNonce[address] = [nonce, new Date()];
        }
      }

      await Promise.all(
        Object.keys(this.#addressToNonce).map(async (address) => {
          await this.mergeNonceFromEVMNode(address);
        })
      );

      this.#initialized = true;
    }
  }

  async mergeNonceFromEVMNode(ethAddress: string): Promise<void> {
    /*
    Retrieves and saves the nonce from the last successful transaction from the EVM node.
    If time period of the last stored nonce exceeds the localNonceTTL, we update the nonce using the getTransactionCount
    call.
    */
    if (this._provider !== null) {
      const lastMergeTimestamp: number = this.#addressToNonce[ethAddress]
        ? this.#addressToNonce[ethAddress][1].getTime()
        : 0;
      const now: number = new Date().getTime();
      const diffInSeconds = (now - lastMergeTimestamp) / 1000;
      if (diffInSeconds < this.#localNonceTTL) {
        return;
      }

      console.log('Fetching latest transaction count from EVM node...');
      const externalNonce: number =
        (await this._provider.getTransactionCount(ethAddress)) - 1;

      const currentLeadingNonce: number =
        this.#addressToLeadingNonce[ethAddress][0];

      // TODO: Includes an expiry for any of the leading nonces. Allows for reuse of stale nonce
      // const currentLeadingNonceTimestamp: number =
      //   this.#addressToLeadingNonce[ethAddress][1].getTime();

      const externalLeadingNonce: number =
        currentLeadingNonce > externalNonce
          ? currentLeadingNonce
          : externalNonce;

      this.#addressToNonce[ethAddress] = [externalNonce, new Date()];
      this.#addressToLeadingNonce[ethAddress] = [
        externalLeadingNonce,
        new Date(),
      ];

      await this.#db.saveNonce(
        this.#chainName,
        this.#chainId,
        ethAddress,
        externalNonce
      );

      await this.#db.saveNonce(
        this.#chainName,
        this.#chainId,
        `${ethAddress}/leading`,
        externalLeadingNonce
      );
    } else {
      logger.error(
        'EVMNonceManager.mergeNonceFromEVMNode called before initiated'
      );
      throw new InitializationError(
        SERVICE_UNITIALIZED_ERROR_MESSAGE(
          'EVMNonceManager.mergeNonceFromEVMNode'
        ),
        SERVICE_UNITIALIZED_ERROR_CODE
      );
    }
  }

  async getNonce(ethAddress: string): Promise<number> {
    /*
    Returns the nonce of the last successful transaction of a given wallet.
    Retrieves the nonce via an EVM call if not already initialized.
    */
    if (this._provider !== null) {
      if (this.#addressToNonce[ethAddress]) {
        await this.mergeNonceFromEVMNode(ethAddress);
        return this.#addressToNonce[ethAddress][0];
      } else {
        const nonce: number =
          (await this._provider.getTransactionCount(ethAddress)) - 1;

        this.#addressToNonce[ethAddress] = [nonce, new Date()];
        await this.#db.saveNonce(
          this.#chainName,
          this.#chainId,
          ethAddress,
          nonce
        );
        return nonce;
      }
    } else {
      logger.error('EVMNonceManager.getNonce called before initiated');
      throw new InitializationError(
        SERVICE_UNITIALIZED_ERROR_MESSAGE('EVMNonceManager.getNonce'),
        SERVICE_UNITIALIZED_ERROR_CODE
      );
    }
  }

  async getNextNonce(ethAddress: string): Promise<number> {
    /*
    Retrieves the next available nonce for a given wallet address.
    This function will automatically increment the leading Nonce of the given wallet address.
    */
    let newNonce;
    if (this._provider !== null) {
      if (this.#addressToLeadingNonce[ethAddress]) {
        await this.mergeNonceFromEVMNode(ethAddress);
        newNonce = this.#addressToLeadingNonce[ethAddress][0] + 1;
      } else {
        newNonce = (await this.getNonce(ethAddress)) + 1;
      }
      this.#addressToLeadingNonce[ethAddress] = [newNonce, new Date()];

      await this.#db.saveNonce(
        this.#chainName,
        this.#chainId,
        `${ethAddress}/leading`,
        newNonce
      );

      return newNonce;
    } else {
      logger.error('EVMNonceManager.getNextNonce called before initiated');
      throw new InitializationError(
        SERVICE_UNITIALIZED_ERROR_MESSAGE('EVMNonceManager.getNextNonce'),
        SERVICE_UNITIALIZED_ERROR_CODE
      );
    }
  }

  async commitNonce(ethAddress: string, txNonce: number): Promise<void> {
    /*
    Stores the nonce of the last successful transaction.
    */
    if (this._provider !== null) {
      this.#addressToNonce[ethAddress] = [txNonce, new Date()];
      await this.#db.saveNonce(
        this.#chainName,
        this.#chainId,
        ethAddress,
        txNonce
      );
    } else {
      logger.error('EVMNonceManager.commitNonce called before initiated');
      throw new InitializationError(
        SERVICE_UNITIALIZED_ERROR_MESSAGE('EVMNonceManager.commitNonce'),
        SERVICE_UNITIALIZED_ERROR_CODE
      );
    }
  }

  async isValidNonce(ethAddress: string, _nonce: number): Promise<boolean> {
    const expectedNonce: number = await this.getNextNonce(ethAddress);
    if (_nonce == expectedNonce) return true;
    return false;
  }

  async close(): Promise<void> {
    await this.#db.close();
  }
}
