def process_string(S):
    rows = S.split('\n')
    header = rows[0]
    processed_rows = [row for row in rows[1:] if not any("NULL" == field for field in row.split(','))]
    result = '\n'.join([header] + processed_rows)
    return result

S = "header,header\nANNUL,ANNULLED\nnull,NILL\nNULL,NULL"
result = process_string(S)
print(result)