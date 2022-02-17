import {
  isFloatString,
  mkValidator,
  mkRequestValidator,
  RequestValidator,
  Validator,
} from '../services/validators';

import {
  validateNonce,
  validateAddress,
  validateMaxFeePerGas,
  validateMaxPriorityFeePerGas,
} from '../chains/ethereum/ethereum.validators';

export const invalidConnectorError: string =
  'The connector param is not a string.';

export const invalidChainError: string = 'The chain param is not a string.';

export const invalidNetworkError: string = 'The network param is not a string.';

export const invalidQuoteError: string = 'The quote param is not a string.';

export const invalidBaseError: string = 'The base param is not a string.';

export const invalidToken: string = 'The token param is not a string';

export const invalidDeadline: string = 'The deadline param is not a string';

export const invalidLiquidityError: string =
  'The liquidity param is not a string';

export const invalidAmountError: string =
  'The amount param must be a string of a non-negative integer.';

export const invalidSideError: string =
  'The side param must be a string of "BUY" or "SELL".';

export const invalidLimitPriceError: string =
  'The limitPrice param may be null or a string of a float or integer number.';

export const validateConnector: Validator = mkValidator(
  'connector',
  invalidConnectorError,
  (val) => typeof val === 'string'
);

export const validateChain: Validator = mkValidator(
  'chain',
  invalidChainError,
  (val) => typeof val === 'string'
);

export const validateNetwork: Validator = mkValidator(
  'network',
  invalidNetworkError,
  (val) => typeof val === 'string'
);

export const validateQuote: Validator = mkValidator(
  'quote',
  invalidQuoteError,
  (val) => typeof val === 'string'
);

export const validateBase: Validator = mkValidator(
  'base',
  invalidBaseError,
  (val) => typeof val === 'string'
);

export const validateLiquidity: Validator = mkValidator(
  'liquidity',
  invalidLiquidityError,
  (val) => typeof val === 'string'
);

export const validateAmount: Validator = mkValidator(
  'amount',
  invalidAmountError,
  (val) => typeof val === 'string' && isFloatString(val)
);

export const validateAmountADesired: Validator = mkValidator(
  'amountADesired',
  invalidAmountError,
  (val) => typeof val === 'string' && isFloatString(val)
);

export const validateAmountBDesired: Validator = mkValidator(
  'amountBDesired',
  invalidAmountError,
  (val) => typeof val === 'string' && isFloatString(val)
);

export const validateAmountAMin: Validator = mkValidator(
  'amountAMin',
  invalidAmountError,
  (val) => typeof val === 'string' && isFloatString(val)
);

export const validateAmountBMin: Validator = mkValidator(
  'amountBMin',
  invalidAmountError,
  (val) => typeof val === 'string' && isFloatString(val)
);

export const validateSide: Validator = mkValidator(
  'side',
  invalidSideError,
  (val) => typeof val === 'string' && (val === 'BUY' || val === 'SELL')
);

export const validateTokenA: Validator = mkValidator(
  'tokenA',
  invalidToken,
  (val) => typeof val === 'string'
);

export const validateTokenB: Validator = mkValidator(
  'tokenB',
  invalidToken,
  (val) => typeof val === 'string'
);

export const validateDeadline: Validator = mkValidator(
  'deadline',
  invalidDeadline,
  (val) => typeof val === 'string',
  true
);

export const validateLimitPrice: Validator = mkValidator(
  'limitPrice',
  invalidLimitPriceError,
  (val) => typeof val === 'string' && isFloatString(val),
  true
);

export const validatePriceRequest: RequestValidator = mkRequestValidator([
  validateConnector,
  validateChain,
  validateNetwork,
  validateQuote,
  validateBase,
  validateAmount,
  validateSide,
]);

export const validateTradeRequest: RequestValidator = mkRequestValidator([
  validateConnector,
  validateChain,
  validateNetwork,
  validateQuote,
  validateBase,
  validateAmount,
  validateAddress,
  validateSide,
  validateLimitPrice,
  validateNonce,
  validateMaxFeePerGas,
  validateMaxPriorityFeePerGas,
]);

export const validateAddLiquidityRequest: RequestValidator = mkRequestValidator(
  [
    validateConnector,
    validateChain,
    validateNetwork,
    validateAddress,
    validateTokenA,
    validateTokenB,
    validateAmountADesired,
    validateAmountBDesired,
    validateAmountAMin,
    validateAmountBMin,
    validateDeadline,
    validateNonce,
    validateMaxFeePerGas,
    validateMaxPriorityFeePerGas,
  ]
);

export const validateRemoveLiquidityRequest: RequestValidator =
  mkRequestValidator([
    validateConnector,
    validateChain,
    validateNetwork,
    validateAddress,
    validateTokenA,
    validateTokenB,
    validateLiquidity,
    validateAmountAMin,
    validateAmountBMin,
    validateDeadline,
    validateNonce,
    validateMaxFeePerGas,
    validateMaxPriorityFeePerGas,
  ]);
