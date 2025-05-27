def process_csv(csv_content: str) -> str:
    lines = csv_content.splitlines()
    processed_lines = []
    for line in lines:
        if line.startswith('#') or not line.strip():
            processed_lines.append(line)
        else:
            parts = line.split(';')
            if len(parts) >= 14:
                new_line = ';'.join(parts[:9] + [parts[9], parts[10], parts[9], '0', parts[11], parts[12], parts[13]])
                processed_lines.append(new_line)
            else:
                processed_lines.append(line)
    return '\n'.join(processed_lines)
