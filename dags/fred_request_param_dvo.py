from dataclasses import dataclass
@dataclass
class FredRequestParamDvo:
        series : str
        start : str
        end : str
        api_key : str                
        def to_dict(self):
            return {
                'series' : self.series,
                'start' : self.start,
                'end' : self.end,
                'api_key' : self.api_key}
        @staticmethod
        def from_dict(data : dict):
              return FredRequestParamDvo(data['series'], data['start'], data['end'], data['api_key'])
        
