class ErrorEvaluationStrategy:
    def __call__(self, column_name, new_column_name):
        pass


class SumOfAbsoluteDiffences(ErrorEvaluationStrategy):
    def __call__(self, column_name, new_column_name):
        return f' SUM(ABS(`{new_column_name}` - `{column_name}`))'
