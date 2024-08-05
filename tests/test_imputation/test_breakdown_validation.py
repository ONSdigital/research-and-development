from src.utils.breakdown_validation import breakdown_validation, filename_validation
from pandas import DataFrame as pandasDF


class TestBreakdownValidation:
    """Unit tests for breakdown_validation function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "222", "223", "203", "202","204",
        "205", "206", "207","221","209", "219", "220", "210", "211", 
        '212','214','216','242','250','243','244','245','246','218',
        '225', '226', '227','228','229','237','302','303','304',
        '305', '501','503','505','507','502','504','506','508','405',
        '407','409','411', '406','408', '410', '412']

        data = [
            # ['A', 1, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87, 89, 91, 93, 95, 97, 99, 101],  
            # ['B', 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 90, 82, 84, 86, 88, 90, 82, 94, 96, 98, 100],
            ['C', 10, 5, 15, 10, 25, 10, 10, 5, 10, 5, 15, 10, 30, 5, 35, 5, 5, 5, 5, 5, 5, 1, 2, 2, 35, 5, 5, 5, 5, 5, 10, 50, 60, 20, 130, 100, 25, 50, 175, 80, 50, 20, 150, 90, 30, 10, 130, 85, 25, 50],
]
        

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df


    def test_breakdown_validation(self):
        "Test for breakdown_validation function."
        input_df = self.create_input_df()
        input_df.head()
        bool_dict, msg = filename_validation(input_df)    
