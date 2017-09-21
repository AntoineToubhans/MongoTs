def assertDataframeHasExpectedColumns(df):
    assert {'count', 'min', 'max', 'mean', 'std'} == set(df.columns)
