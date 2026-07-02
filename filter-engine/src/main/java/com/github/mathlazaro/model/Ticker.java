package com.github.mathlazaro.model;

public enum Ticker {
    PETR4_SA,
    PETR3_SA,
    PRIO3_SA,
    UGPA3_SA,
    VALE3_SA,
    CSNA3_SA,
    GGBR4_SA,
    ITUB4_SA,
    BBDC4_SA,
    BBAS3_SA,
    SANB11_SA,
    B3SA3_SA,
    IRBR3_SA,
    TOTVS3_SA,
    LWSA3_SA,
    INTB3_SA,
    MGLU3_SA,
    VIIA3_SA,
    RENT3_SA,
    LREN3_SA,
    RDRD3_SA,
    HAPV3_SA,
    FLRY3_SA,
    VIVT3_SA,
    TIMS3_SA,
    ELET3_SA,
    ELET6_SA,
    CPFE3_SA,
    SBSP3_SA,
    AGRO3_SA,
    SLCE3_SA,
    AAPL,
    MSFT,
    NVDA,
    GOOGL,
    META,
    AMZN,
    TSLA,
    AMD,
    INTC,
    CRM,
    JPM,
    BAC,
    GS,
    MS,
    BRK_B,
    XOM,
    CVX,
    JNJ,
    UNH,
    PFE,
    WMT,
    COST,
    MCD,
    KO,
    BTC_USD,
    ETH_USD,
    BNB_USD,
    SOL_USD,
    XRP_USD,
    DOGE_USD,
    ADA_USD,
    AVAX_USD,
    TRX_USD,
    TON_USD,
    ANY,
    UNKNOWN;

    public static Ticker fromString(String sector) {

        if (sector == null) {
            return Ticker.UNKNOWN;
        }
        for (Ticker s : Ticker.values()) {
            if (s.name().equalsIgnoreCase(sector)) {
                return s;
            }
        }
        return Ticker.UNKNOWN;
    }


}
