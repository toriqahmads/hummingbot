import Decimal from 'decimal.js-light';
import { BigNumber, Wallet } from 'ethers';
import {
  HttpException,
  LOAD_WALLET_ERROR_CODE,
  LOAD_WALLET_ERROR_MESSAGE,
  TOKEN_NOT_SUPPORTED_ERROR_CODE,
  TOKEN_NOT_SUPPORTED_ERROR_MESSAGE,
  TRADE_FAILED_ERROR_CODE,
  TRADE_FAILED_ERROR_MESSAGE,
  SWAP_PRICE_EXCEEDS_LIMIT_PRICE_ERROR_CODE,
  SWAP_PRICE_EXCEEDS_LIMIT_PRICE_ERROR_MESSAGE,
  SWAP_PRICE_LOWER_THAN_LIMIT_PRICE_ERROR_CODE,
  SWAP_PRICE_LOWER_THAN_LIMIT_PRICE_ERROR_MESSAGE,
  UNKNOWN_ERROR_ERROR_CODE,
  UNKNOWN_ERROR_MESSAGE,
} from '../../services/error-handler';
import { TokenInfo } from '../../services/ethereum-base';
import { latency, gasCostInEthString } from '../../services/base';
import {
  Ethereumish,
  ExpectedTrade,
  Uniswapish,
  Tokenish,
} from '../../services/common-interfaces';
import {
  PriceRequest,
  PriceResponse,
  TradeRequest,
  TradeResponse,
} from '../../amm/amm.requests';

export interface TradeInfo {
  baseToken: Tokenish;
  quoteToken: Tokenish;
  requestAmount: BigNumber;
  expectedTrade: ExpectedTrade;
}

export async function getTradeInfo(
  ethereumish: Ethereumish,
  uniswapish: Uniswapish,
  baseAsset: string,
  quoteAsset: string,
  baseAmount: Decimal,
  tradeSide: string
): Promise<TradeInfo> {
  const baseToken: Tokenish = getFullTokenFromSymbol(
    ethereumish,
    uniswapish,
    baseAsset
  );
  const quoteToken: Tokenish = getFullTokenFromSymbol(
    ethereumish,
    uniswapish,
    quoteAsset
  );
  const requestAmount: BigNumber = BigNumber.from(
    baseAmount.toFixed(baseToken.decimals).replace('.', '')
  );

  let expectedTrade: ExpectedTrade;
  if (tradeSide === 'BUY') {
    expectedTrade = await uniswapish.estimateBuyTrade(
      quoteToken,
      baseToken,
      requestAmount
    );
  } else {
    expectedTrade = await uniswapish.estimateSellTrade(
      baseToken,
      quoteToken,
      requestAmount
    );
  }

  return {
    baseToken,
    quoteToken,
    requestAmount,
    expectedTrade,
  };
}

export async function price(
  ethereumish: Ethereumish,
  uniswapish: Uniswapish,
  req: PriceRequest
): Promise<PriceResponse> {
  const startTimestamp: number = Date.now();
  let tradeInfo: TradeInfo;
  try {
    tradeInfo = await getTradeInfo(
      ethereumish,
      uniswapish,
      req.base,
      req.quote,
      new Decimal(req.amount),
      req.side
    );
  } catch (e) {
    if (e instanceof Error) {
      throw new HttpException(
        500,
        TRADE_FAILED_ERROR_MESSAGE + e.message,
        TRADE_FAILED_ERROR_CODE
      );
    } else {
      throw new HttpException(
        500,
        UNKNOWN_ERROR_MESSAGE,
        UNKNOWN_ERROR_ERROR_CODE
      );
    }
  }

  const trade = tradeInfo.expectedTrade.trade;
  const expectedAmount = tradeInfo.expectedTrade.expectedAmount;

  const tradePrice =
    req.side === 'BUY' ? trade.executionPrice.invert() : trade.executionPrice;

  const gasLimit = uniswapish.gasLimit;
  const gasPrice = ethereumish.gasPrice;
  return {
    network: ethereumish.chain,
    timestamp: startTimestamp,
    latency: latency(startTimestamp, Date.now()),
    base: tradeInfo.baseToken.address,
    quote: tradeInfo.quoteToken.address,
    amount: new Decimal(req.amount).toFixed(tradeInfo.baseToken.decimals),
    rawAmount: tradeInfo.requestAmount.toString(),
    expectedAmount: expectedAmount.toSignificant(8),
    price: tradePrice.toSignificant(8),
    gasPrice: gasPrice,
    gasLimit: gasLimit,
    gasCost: gasCostInEthString(gasPrice, gasLimit),
  };
}

export async function trade(
  ethereumish: Ethereumish,
  uniswapish: Uniswapish,
  req: TradeRequest
): Promise<TradeResponse> {
  const startTimestamp: number = Date.now();

  const { limitPrice, maxFeePerGas, maxPriorityFeePerGas } = req;

  let maxFeePerGasBigNumber;
  if (maxFeePerGas) {
    maxFeePerGasBigNumber = BigNumber.from(maxFeePerGas);
  }
  let maxPriorityFeePerGasBigNumber;
  if (maxPriorityFeePerGas) {
    maxPriorityFeePerGasBigNumber = BigNumber.from(maxPriorityFeePerGas);
  }

  let wallet: Wallet;
  try {
    wallet = await ethereumish.getWallet(req.address);
  } catch (err) {
    throw new HttpException(
      500,
      LOAD_WALLET_ERROR_MESSAGE + err,
      LOAD_WALLET_ERROR_CODE
    );
  }

  let tradeInfo: TradeInfo;
  try {
    tradeInfo = await getTradeInfo(
      ethereumish,
      uniswapish,
      req.base,
      req.quote,
      new Decimal(req.amount),
      req.side
    );
  } catch (e) {
    if (e instanceof Error) {
      throw new HttpException(
        500,
        TRADE_FAILED_ERROR_MESSAGE + e.message,
        TRADE_FAILED_ERROR_CODE
      );
    } else {
      throw new HttpException(
        500,
        UNKNOWN_ERROR_MESSAGE,
        UNKNOWN_ERROR_ERROR_CODE
      );
    }
  }

  const gasPrice: number = ethereumish.gasPrice;
  const gasLimit: number = uniswapish.gasLimit;

  if (req.side === 'BUY') {
    const price = tradeInfo.expectedTrade.trade.executionPrice.invert();
    if (
      limitPrice &&
      new Decimal(price.toFixed(8).toString()).gte(new Decimal(limitPrice))
    )
      throw new HttpException(
        500,
        SWAP_PRICE_EXCEEDS_LIMIT_PRICE_ERROR_MESSAGE(price, limitPrice),
        SWAP_PRICE_EXCEEDS_LIMIT_PRICE_ERROR_CODE
      );

    const tx = await uniswapish.executeTrade(
      wallet,
      tradeInfo.expectedTrade.trade,
      gasPrice,
      uniswapish.router,
      uniswapish.ttl,
      uniswapish.routerAbi,
      uniswapish.gasLimit,
      req.nonce,
      maxFeePerGasBigNumber,
      maxPriorityFeePerGasBigNumber
    );

    if (tx.hash) {
      await ethereumish.txStorage.saveTx(
        ethereumish.chain,
        ethereumish.chainId,
        tx.hash,
        new Date(),
        ethereumish.gasPrice
      );
    }

    return {
      network: ethereumish.chain,
      timestamp: startTimestamp,
      latency: latency(startTimestamp, Date.now()),
      base: tradeInfo.baseToken.address,
      quote: tradeInfo.quoteToken.address,
      amount: new Decimal(req.amount).toFixed(tradeInfo.baseToken.decimals),
      rawAmount: tradeInfo.requestAmount.toString(),
      expectedIn: tradeInfo.expectedTrade.expectedAmount.toSignificant(8),
      price: price.toSignificant(8),
      gasPrice: gasPrice,
      gasLimit: gasLimit,
      gasCost: gasCostInEthString(gasPrice, gasLimit),
      nonce: tx.nonce,
      txHash: tx.hash,
    };
  } else {
    const price = tradeInfo.expectedTrade.trade.executionPrice;
    if (
      limitPrice &&
      new Decimal(price.toFixed(8).toString()).gte(new Decimal(limitPrice))
    )
      throw new HttpException(
        500,
        SWAP_PRICE_LOWER_THAN_LIMIT_PRICE_ERROR_MESSAGE(price, limitPrice),
        SWAP_PRICE_LOWER_THAN_LIMIT_PRICE_ERROR_CODE
      );

    const tx = await uniswapish.executeTrade(
      wallet,
      tradeInfo.expectedTrade.trade,
      gasPrice,
      uniswapish.router,
      uniswapish.ttl,
      uniswapish.routerAbi,
      uniswapish.gasLimit,
      req.nonce,
      maxFeePerGasBigNumber,
      maxPriorityFeePerGasBigNumber
    );
    return {
      network: ethereumish.chain,
      timestamp: startTimestamp,
      latency: latency(startTimestamp, Date.now()),
      base: tradeInfo.baseToken.address,
      quote: tradeInfo.quoteToken.address,
      amount: new Decimal(req.amount).toFixed(tradeInfo.baseToken.decimals),
      rawAmount: tradeInfo.requestAmount.toString(),
      expectedOut: tradeInfo.expectedTrade.expectedAmount.toSignificant(8),
      price: price.toSignificant(8),
      gasPrice: gasPrice,
      gasLimit,
      gasCost: gasCostInEthString(gasPrice, gasLimit),
      nonce: tx.nonce,
      txHash: tx.hash,
    };
  }
}

export function getFullTokenFromSymbol(
  ethereumish: Ethereumish,
  uniswapish: Uniswapish,
  tokenSymbol: string
): Tokenish {
  const tokenInfo: TokenInfo | undefined =
    ethereumish.getTokenBySymbol(tokenSymbol);
  let fullToken: Tokenish | undefined;
  if (tokenInfo) {
    fullToken = uniswapish.getTokenByAddress(tokenInfo.address);
  }
  if (!fullToken)
    throw new HttpException(
      500,
      TOKEN_NOT_SUPPORTED_ERROR_MESSAGE + tokenSymbol,
      TOKEN_NOT_SUPPORTED_ERROR_CODE
    );
  return fullToken;
}