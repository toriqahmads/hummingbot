import express from 'express';
import { Express } from 'express-serve-static-core';
import request from 'supertest';
import { NewEthereum } from '../../../../src/chains/ethereum/new_ethereum';
import { NewUniswap } from '../../../../src/connectors/uniswap/new_uniswap';
import { TradingRoutes } from '../../../../src/trading/trading.routes';
import { patch, unpatch } from '../../../services/patch';

let app: Express;
let ethereum: NewEthereum;
let uniswap: NewUniswap;

beforeAll(async () => {
  app = express();
  app.use(express.json());
  ethereum = NewEthereum.getInstance('kovan');
  await ethereum.init();
  uniswap = NewUniswap.getInstance('ethereum', 'kovan');
  await uniswap.init();
  app.use('/trading', TradingRoutes.router);
});

afterEach(() => {
  unpatch();
});

const address: string = '0xFaA12FD102FE8623C9299c72B03E45107F2772B5';

const patchGetWallet = () => {
  patch(ethereum, 'getWallet', () => {
    return {
      address: '0xFaA12FD102FE8623C9299c72B03E45107F2772B5',
    };
  });
};

const patchInit = () => {
  patch(uniswap, 'init', async () => {
    return;
  });
};

const patchStoredTokenList = () => {
  patch(ethereum, 'tokenList', () => {
    return [
      {
        chainId: 42,
        name: 'WETH',
        symbol: 'WETH',
        address: '0xd0A1E359811322d97991E03f863a0C30C2cF029C',
        decimals: 18,
      },
      {
        chainId: 42,
        name: 'DAI',
        symbol: 'DAI',
        address: '0x4f96fe3b7a6cf9725f59d353f723c1bdb64ca6aa',
        decimals: 18,
      },
    ];
  });
};

const patchGetTokenBySymbol = () => {
  patch(ethereum, 'getTokenBySymbol', (symbol: string) => {
    if (symbol === 'WETH') {
      return {
        chainId: 42,
        name: 'WETH',
        symbol: 'WETH',
        address: '0xd0A1E359811322d97991E03f863a0C30C2cF029C',
        decimals: 18,
      };
    } else {
      return {
        chainId: 42,
        name: 'DAI',
        symbol: 'DAI',
        address: '0x4f96fe3b7a6cf9725f59d353f723c1bdb64ca6aa',
        decimals: 18,
      };
    }
  });
};

const patchGetTokenByAddress = () => {
  patch(uniswap, 'getTokenByAddress', () => {
    return {
      chainId: 42,
      name: 'WETH',
      symbol: 'WETH',
      address: '0xd0A1E359811322d97991E03f863a0C30C2cF029C',
      decimals: 18,
    };
  });
};

const patchGasPrice = () => {
  patch(ethereum, 'gasPrice', () => 100);
};

const patchPriceSwapOut = () => {
  patch(uniswap, 'priceSwapOut', () => {
    return {
      expectedAmount: {
        toSignificant: () => 100,
      },
      trade: {
        executionPrice: {
          invert: jest.fn().mockReturnValue({
            toSignificant: () => 100,
            toFixed: () => '100',
          }),
        },
      },
    };
  });
};

const patchPriceSwapIn = () => {
  patch(uniswap, 'priceSwapIn', () => {
    return {
      expectedAmount: {
        toSignificant: () => 100,
      },
      trade: {
        executionPrice: {
          toSignificant: () => 100,
          toFixed: () => '100',
        },
      },
    };
  });
};

const patchGetNonce = () => {
  patch(ethereum.nonceManager, 'getNonce', () => 21);
};

const patchExecuteTrade = () => {
  patch(uniswap, 'executeTrade', () => {
    return { nonce: 21, hash: '000000000000000' };
  });
};

// describe('GET /uniswap', () => {
//   it('should return 200', async () => {
//     patchInit();
//     await request(app)
//       .get(`/eth/uniswap`)
//       .expect('Content-Type', /json/)
//       .expect(200)
//       .expect((res) => expect(res.body.connection).toBe(true));
//   });
// });

describe('POST /trading/price', () => {
  it('should return 200 for BUY', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patchGasPrice();
    patchPriceSwapOut();
    patchGetNonce();
    patchExecuteTrade();

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        side: 'BUY',
      })
      .set('Accept', 'application/json')
      .expect(200)
      .then((res: any) => {
        expect(res.body.amount).toEqual('10000');
      });
  });

  it('should return 200 for SELL', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patchGasPrice();
    patchPriceSwapIn();
    patchGetNonce();
    patchExecuteTrade();

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        side: 'SELL',
      })
      .set('Accept', 'application/json')
      .expect(200)
      .then((res: any) => {
        expect(res.body.amount).toEqual('10000');
      });
  });

  it('should return 500 for unrecognized quote symbol', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DOGE',
        base: 'WETH',
        amount: '10000',
        side: 'SELL',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 for unrecognized base symbol', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'SHIBA',
        amount: '10000',
        side: 'SELL',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 for unrecognized base symbol with decimals in the amount and SELL', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'SHIBA',
        amount: '10.000',
        side: 'SELL',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 for unrecognized base symbol with decimals in the amount and BUY', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'SHIBA',
        amount: '10.000',
        side: 'BUY',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 when the priceSwapIn operation fails', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patch(uniswap, 'priceSwapIn', () => {
      return 'error';
    });

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DOGE',
        base: 'WETH',
        amount: '10000',
        side: 'SELL',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 when the priceSwapOut operation fails', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patch(uniswap, 'priceSwapOut', () => {
      return 'error';
    });

    await request(app)
      .post(`/trading/price`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DOGE',
        base: 'WETH',
        amount: '10000',
        side: 'BUY',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });
});

describe('POST /trading/trade', () => {
  const patchForBuy = () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patchGasPrice();
    patchPriceSwapOut();
    patchGetNonce();
    patchExecuteTrade();
  };
  it('should return 200 for BUY', async () => {
    patchForBuy();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'BUY',
        nonce: 21,
      })
      .set('Accept', 'application/json')
      .expect(200)
      .then((res: any) => {
        expect(res.body.nonce).toEqual(21);
      });
  });

  it('should return 200 for BUY without nonce parameter', async () => {
    patchForBuy();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'BUY',
      })
      .set('Accept', 'application/json')
      .expect(200);
  });

  it('should return 200 for BUY with maxFeePerGas and maxPriorityFeePerGas', async () => {
    patchForBuy();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'BUY',
        nonce: 21,
        maxFeePerGas: '5000000000',
        maxPriorityFeePerGas: '5000000000',
      })
      .set('Accept', 'application/json')
      .expect(200);
  });

  const patchForSell = () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patchGasPrice();
    patchPriceSwapIn();
    patchGetNonce();
    patchExecuteTrade();
  };
  it('should return 200 for SELL', async () => {
    patchForSell();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'SELL',
        nonce: 21,
      })
      .set('Accept', 'application/json')
      .expect(200)
      .then((res: any) => {
        expect(res.body.nonce).toEqual(21);
      });
  });

  it('should return 200 for SELL  with maxFeePerGas and maxPriorityFeePerGas', async () => {
    patchForSell();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'SELL',
        nonce: 21,
        maxFeePerGas: '5000000000',
        maxPriorityFeePerGas: '5000000000',
      })
      .set('Accept', 'application/json')
      .expect(200);
  });

  it('should return 200 for SELL with limitPrice', async () => {
    patchForSell();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'SELL',
        nonce: 21,
        limitPrice: '999999999999999999999',
      })
      .set('Accept', 'application/json')
      .expect(200);
  });

  it('should return 200 for BUY with limitPrice', async () => {
    patchForBuy();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'BUY',
        nonce: 21,
        limitPrice: '999999999999999999999',
      })
      .set('Accept', 'application/json')
      .expect(200);
  });

  it('should return 500 for BUY with price smaller than limitPrice', async () => {
    patchForBuy();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'BUY',
        nonce: 21,
        limitPrice: '9',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 for SELL with price smaller than limitPrice', async () => {
    patchForSell();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'SELL',
        nonce: 21,
        limitPrice: '9',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 404 when parameters are incorrect', async () => {
    patchInit();
    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: 10000,
        address: 'da8',
        side: 'comprar',
      })
      .set('Accept', 'application/json')
      .expect(404);
  });
  it('should return 500 when the priceSwapIn operation fails', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patch(uniswap, 'priceSwapIn', () => {
      return 'error';
    });

    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'SELL',
        nonce: 21,
        maxFeePerGas: '5000000000',
        maxPriorityFeePerGas: '5000000000',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });

  it('should return 500 when the priceSwapOut operation fails', async () => {
    patchGetWallet();
    patchInit();
    patchStoredTokenList();
    patchGetTokenBySymbol();
    patchGetTokenByAddress();
    patch(uniswap, 'priceSwapOut', () => {
      return 'error';
    });

    await request(app)
      .post(`/trading/trade`)
      .send({
        chain: 'ethereum',
        network: 'kovan',
        connector: 'uniswap',
        quote: 'DAI',
        base: 'WETH',
        amount: '10000',
        address,
        side: 'BUY',
        nonce: 21,
        maxFeePerGas: '5000000000',
        maxPriorityFeePerGas: '5000000000',
      })
      .set('Accept', 'application/json')
      .expect(500);
  });
});