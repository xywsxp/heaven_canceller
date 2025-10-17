use serde::{Deserialize, Serialize};
use utils::clickhouse_events::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct EventBundle {
    pub pumpfun_trade_event: Vec<PumpfunTradeEventV2>,
    pub pumpfun_create_event: Vec<PumpfunCreateEventV2>,
    pub pumpfun_migrate_event: Vec<PumpfunMigrateEventV2>,
    pub pumpfun_amm_buy_event: Vec<PumpfunAmmBuyEventV2>,
    pub pumpfun_amm_sell_event: Vec<PumpfunAmmSellEventV2>,
    pub pumpfun_amm_create_pool_event: Vec<PumpfunAmmCreatePoolEventV2>,
    pub pumpfun_amm_deposit_event: Vec<PumpfunAmmDepositEventV2>,
    pub pumpfun_amm_withdraw_event: Vec<PumpfunAmmWithdrawEventV2>,
}

impl EventBundle {
    pub fn is_empty(&self) -> bool {
        self.pumpfun_trade_event.is_empty()
            && self.pumpfun_create_event.is_empty()
            && self.pumpfun_migrate_event.is_empty()
            && self.pumpfun_amm_buy_event.is_empty()
            && self.pumpfun_amm_sell_event.is_empty()
            && self.pumpfun_amm_create_pool_event.is_empty()
            && self.pumpfun_amm_deposit_event.is_empty()
            && self.pumpfun_amm_withdraw_event.is_empty()
    }
}

impl Default for EventBundle {
    fn default() -> Self {
        Self {
            pumpfun_trade_event: Vec::new(),
            pumpfun_create_event: Vec::new(),
            pumpfun_migrate_event: Vec::new(),
            pumpfun_amm_buy_event: Vec::new(),
            pumpfun_amm_sell_event: Vec::new(),
            pumpfun_amm_create_pool_event: Vec::new(),
            pumpfun_amm_deposit_event: Vec::new(),
            pumpfun_amm_withdraw_event: Vec::new(),
        }
    }
}
