"""Constants for the ANWB Energie Account integration."""

DOMAIN = "anwb_energie_account"
CLIENT_ID = "57fe1448-00e6-47f2-bb50-c0935640b1fa"
OAUTH2_AUTHORIZE = (
    "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/authorize"
)
OAUTH2_TOKEN = "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/token"

KRAKEN_TOKEN_URL = "https://api.anwb.nl/energy/energy-services/v1/auth/kraken-token"
GRAPHQL_URL = "https://api.anwb-kraken.energy/v1/graphql/"

# Fixed Costs Fallbacks
VASTE_LEVERINGSKOSTEN = 8.50
NETBEHEERKOSTEN = 39.73
VERMINDERING_ENERGIEBELASTING = -52.41

VASTE_LEVERINGSKOSTEN_GAS = 8.50
NETBEHEERKOSTEN_GAS = 17.50
VERMINDERING_ENERGIEBELASTING_GAS = 0.0

OAUTH2_SCOPES = [
    "openid",
    "profile",
    "email",
    "offline_access",
]
