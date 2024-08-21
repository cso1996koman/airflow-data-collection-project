from dataclasses import dataclass
@dataclass
class YFinanceUsdToKrwExchangeRateRequestParamDvo:
        ticker : str
        start : str
        end : str
        interval : str
        period : str
        api_key : str
        def to_dict(self):
            return {
                "ticker": self.ticker,
                "start": self.start,
                "end": self.end,
                "interval": self.interval,
                "period": self.period,
                "api_key": self.api_key
            }
        @staticmethod
        def from_dict(data_dict : dict):
              return YFinanceUsdToKrwExchangeRateRequestParamDvo(
                  ticker=data_dict['ticker'],
                  start=data_dict['start'],
                  end=data_dict['end'],
                  interval=data_dict['interval'],
                  period=data_dict['period'],
                  api_key=data_dict['api_key']
              )



