class Processor:
    def __init__(self):
        self.date = None

    def process(self, data):
        self.date = data["year"] + "-" + data["month"] + "-" + data["day"]
        return {"date": self.date, "event": data["event"]}
