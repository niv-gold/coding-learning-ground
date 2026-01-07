
class NYCtaxiELT():



    def __init__(self):
        pass

    def extract(self):
        pass
    
    def load(self):
        pass

    def transform(self):
        '''DBT will be used as the transformation tool'''
        pass

    def run(self):
        self.extract()
        self.load()
        print("ELT process completed.")

if __name__ == "__main__":
    elt = NYCtaxiELT()
    elt.run()