jest.useFakeTimers();
import { Uniswap } from '../../../../src/connectors/uniswap/uniswap';
import { patch, unpatch } from '../../../services/patch';
import { UniswapishPriceError } from '../../../../src/services/error-handler';
import {
  Fetcher,
  Pair,
  Route,
  Token,
  TokenAmount,
  Trade,
  TradeType,
} from '@uniswap/sdk';
import { BigNumber } from 'ethers';
import { Ethereum } from '../../../../src/chains/ethereum/ethereum';

let ethereum: Ethereum;
let uniswap: Uniswap;

const WETH = new Token(
  3,
  '0xd0A1E359811322d97991E03f863a0C30C2cF029C',
  18,
  'WETH'
);
const DAI = new Token(
  3,
  '0x4f96fe3b7a6cf9725f59d353f723c1bdb64ca6aa',
  18,
  'DAI'
);

beforeAll(async () => {
  ethereum = Ethereum.getInstance('kovan');
  await ethereum.init();
  uniswap = Uniswap.getInstance('ethereum', 'kovan');
  patch(uniswap, '_poolStrings', []); // this avoids uniswap from trying to download pool data
  await uniswap.init();
});

afterEach(() => {
  unpatch();
});

const patchFetchPairData = () => {
  patch(Fetcher, 'fetchPairData', () => {
    return new Pair(
      new TokenAmount(WETH, '2000000000000000000'),
      new TokenAmount(DAI, '1000000000000000000')
    );
  });
};

const mkDaiToWethTrade = (): Trade => {
  const WETH_DAI = new Pair(
    new TokenAmount(WETH, '2000000000000000000'),
    new TokenAmount(DAI, '1000000000000000000')
  );
  const DAI_TO_WETH = new Route([WETH_DAI], DAI);
  return new Trade(
    DAI_TO_WETH,
    new TokenAmount(DAI, '1000000000000000'),
    TradeType.EXACT_INPUT
  );
};

const patchTrade = (key: string, error?: Error) => {
  patch(Trade, key, () => {
    if (error) return [];
    return [mkDaiToWethTrade()];
  });
};

describe('verify Uniswap estimateSellTrade', () => {
  it('Should return an ExpectedTrade when available', async () => {
    patchFetchPairData();
    patchTrade('bestTradeExactIn');

    const expectedTrade = await uniswap.estimateSellTrade(
      WETH,
      DAI,
      BigNumber.from(1)
    );
    expect(expectedTrade).toHaveProperty('trade');
    expect(expectedTrade).toHaveProperty('expectedAmount');
  });

  it('Should throw an error if no pair is available', async () => {
    patchFetchPairData();
    patchTrade('bestTradeExactIn', new Error('error getting trade'));

    await expect(async () => {
      await uniswap.estimateSellTrade(WETH, DAI, BigNumber.from(1));
    }).rejects.toThrow(UniswapishPriceError);
  });
});

describe('verify Uniswap estimateBuyTrade', () => {
  it('Should return an ExpectedTrade when available', async () => {
    patchFetchPairData();
    patchTrade('bestTradeExactOut');

    const expectedTrade = await uniswap.estimateBuyTrade(
      WETH,
      DAI,
      BigNumber.from(1)
    );
    expect(expectedTrade).toHaveProperty('trade');
    expect(expectedTrade).toHaveProperty('expectedAmount');
  });

  it('Should return an error if no pair is available', async () => {
    patchFetchPairData();
    patchTrade('bestTradeExactOut', new Error('error getting trade'));

    await expect(async () => {
      await uniswap.estimateBuyTrade(WETH, DAI, BigNumber.from(1));
    }).rejects.toThrow(UniswapishPriceError);
  });
});

const patchGetPoolToNull = () => {
  patch(uniswap, 'getPool', (_tokenA: Token, _tokenB: Token) => {
    return null;
  });
};

const patchGetPool = () => {
  patch(uniswap, 'getPool', (_tokenA: Token, _tokenB: Token) => {
    return '0x3D2097889B97A9eF23B3eA8FC10c626fbda29099';
  });
};

describe('getPool', () => {
  it('Return null for non-existant pool', async () => {
    patchGetPoolToNull();
    const address = await uniswap.getPool(
      WETH,
      DAI,
      uniswap.factoryAddress,
      uniswap.factoryAbi
    );
    expect(address).toEqual(null);
  });

  it('Return address for pool', async () => {
    patchGetPool();
    const address = await uniswap.getPool(
      WETH,
      DAI,
      uniswap.factoryAddress,
      uniswap.factoryAbi
    );
    expect(address).toEqual('0x3D2097889B97A9eF23B3eA8FC10c626fbda29099');
  });
});

describe('getTradeRoute', () => {
  it('Return dai to weth trade route', async () => {
    const tradeRoute = uniswap.getTradeRoute(mkDaiToWethTrade());
    expect(tradeRoute).toEqual(['DAI-WETH']);
  });
});
