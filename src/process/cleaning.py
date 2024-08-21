def rename_columns_by_regex(df, to_replace, replace_with):
    return df.rename(lambda column_name: column_name.replace(to_replace, replace_with))
