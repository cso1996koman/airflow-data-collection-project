from dataclasses import dataclass
@dataclass
class YFinanceUsdToKrwExchangeRateRequestParam:
        ticker : str
        start : str
        end : str
        interval : str
        def to_dict(self):
            return {
                "ticker": self.ticker,
                "start": self.start,
                "end": self.end,
                "interval": self.interval
            }
        @staticmethod
        def from_dict(data_dict : dict):
              return YFinanceUsdToKrwExchangeRateRequestParam(
                  ticker=data_dict['ticker'],
                  start=data_dict['start'],
                  end=data_dict['end'],
                  interval=data_dict['interval']
              )



