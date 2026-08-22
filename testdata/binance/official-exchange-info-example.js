{
    "timezone": "UTC",
    "serverTime": 1565246363776,
    "rateLimits": [
        {
            // These are defined in the `ENUM definitions` section under `Rate Limiters (rateLimitType)`.
            // All limits are optional
        }
    ],
    "exchangeFilters": [
        // These are the defined filters in the `Filters` section.
        // All filters are optional.
    ],
    "symbols": [
        {
            "symbol": "ETHBTC",
            "status": "TRADING",
            "baseAsset": "ETH",
            "baseAssetPrecision": 8,
            "quoteAsset": "BTC",
            "quotePrecision": 8,     // will be removed in future api versions (v4+)
            "quoteAssetPrecision": 8,
            "baseCommissionPrecision": 8,
            "quoteCommissionPrecision": 8,
            "orderTypes": [
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT",
                "TAKE_PROFIT_LIMIT"
            ],
            "icebergAllowed": true,
            "ocoAllowed": true,
            "otoAllowed": true,
            "opoAllowed": true,
            "quoteOrderQtyMarketAllowed": true,
            "allowTrailingStop": false,
            "cancelReplaceAllowed": false,
            "amendAllowed": false,
            "pegInstructionsAllowed": true,
            "isSpotTradingAllowed": true,
            "isMarginTradingAllowed": true,
            "filters": [
                // These are defined in the Filters section.
                // All filters are optional
            ],
            "permissions": [],
            "permissionSets": [["SPOT", "MARGIN"]],
            "defaultSelfTradePreventionMode": "NONE",
            "allowedSelfTradePreventionModes": ["NONE"]
        }
    ],
    // Optional field. Present only when SOR is available.
    // https://github.com/binance/binance-spot-api-docs/blob/master/faqs/sor_faq.md
    "sors": [
        {
            "baseAsset": "BTC",
            "symbols": ["BTCUSDT", "BTCUSDC"]
        }
    ]
}
