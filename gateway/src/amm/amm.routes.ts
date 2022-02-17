/* eslint-disable no-inner-declarations */
/* eslint-disable @typescript-eslint/ban-types */
import { Router, Request, Response } from 'express';
import { asyncHandler } from '../services/error-handler';
import { price, trade } from './amm.controllers';
import {
  AddLiquidityRequest,
  AddLiquidityResponse,
  PriceRequest,
  PriceResponse,
  RemoveLiquidityRequest,
  RemoveLiquidityResponse,
  TradeRequest,
  TradeResponse,
} from './amm.requests';
import { validateAddLiquidityRequest, validatePriceRequest, validateRemoveLiquidityRequest, validateTradeRequest } from './amm.validators';

export namespace AmmRoutes {
  export const router = Router();

  router.post(
    '/price',
    asyncHandler(
      async (
        req: Request<{}, {}, PriceRequest>,
        res: Response<PriceResponse | string, {}>
      ) => {
        validatePriceRequest(req.body);
        res.status(200).json(await price(req.body));
      }
    )
  );

  router.post(
    '/trade',
    asyncHandler(
      async (
        req: Request<{}, {}, TradeRequest>,
        res: Response<TradeResponse | string, {}>
      ) => {
        validateTradeRequest(req.body);
        res.status(200).json(await trade(req.body));
      }
    )
  );

  router.post(
    '/liquidity/add',
    asyncHandler(
      async (
        req: Request<{}, {}, AddLiquidityRequest>,
        res: Response<AddLiquidityResponse | string, {}>
      ) => {
        validateAddLiquidityRequest(req.body);
        res.status(200).json(await addLiquidity(req.body));
      }
    )
  );

  router.post(
    '/liquidity/remove',
    asyncHandler(
      async (
        req: Request<{}, {}, RemoveLiquidityRequest>,
        res: Response<RemoveLiquidityResponse | string, {}>
      ) => {
        validateRemoveLiquidityRequest(req.body);
        res.status(200).json(await removeLiquidity(req.body));
      }
    )
  );
}
