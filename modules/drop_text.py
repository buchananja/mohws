class DropText:
    '''contains functionality for dropping unecessary text from station data'''
    
    
    @staticmethod
    def drop_site_closed_row(data_lines):
        '''removes invalid final row'''

        if ('site,closed') in data_lines[-1]:
            data_lines = data_lines[:-1]
        return data_lines
        

    @staticmethod
    def drop_units_row(data_lines):
        '''removes units row'''

        for index, line in enumerate(data_lines[:10]):
            if ('degc') in line:
                data_lines.pop(index)
        return data_lines


    @staticmethod
    def drop_header_text(data_lines):
        '''removes header text'''

        for index, line in enumerate(data_lines[:10]):
            if 'yyyy' in line:
                data_lines = data_lines[index:]
        return data_lines