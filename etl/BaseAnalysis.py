class BaseAnalysis():
    """
    Class that represents a generical analysis.
    Should be the superclass of each analysis.

    ...

    Attributes
    ----------
    data : list of list
        data to be analysed

    Methods
    -------
    analyse():
        The method to be over.
    """
    def __init__(self, data):
        self.data = data

    def analyse(self):
        '''
        This function should be overriden with the implementation of specific analysis.
        '''
        pass