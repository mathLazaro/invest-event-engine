package com.github.mathlazaro.model;

public enum Sector {
    ENERGIA,
    MINERACAO,
    SIDERURGIA,
    PAPEL_CELULOSE,
    FINANCEIRO,
    SEGUROS,
    TECNOLOGIA,
    LOCACAO,
    VAREJO,
    SAUDE,
    TELECOM,
    UTILITIES,
    AGRONEGOCIO,
    CONSUMO,
    CRYPTO,
    ANY,
    UNKNOWN;

    public static Sector fromString(String sector) {

        if (sector == null) {
            return Sector.UNKNOWN;
        }
        for (Sector s : Sector.values()) {
            if (s.name().equalsIgnoreCase(sector)) {
                return s;
            }
        }
        return Sector.UNKNOWN;
    }

}
