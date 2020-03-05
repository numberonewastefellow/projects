#https://stackoverflow.com/questions/7167008/efficiently-finding-the-last-line-in-a-text-file
def last_line(in_file, block_size=1024, ignore_ending_newline=False):
    suffix = ""
    in_file.seek(0, os.SEEK_END)
    in_file_length = in_file.tell()
    seek_offset = 0

    while(-seek_offset < in_file_length):
        # Read from end.
        seek_offset -= block_size
        if -seek_offset > in_file_length:
            # Limit if we ran out of file (can't seek backward from start).
            block_size -= -seek_offset - in_file_length
            if block_size == 0:
                break
            seek_offset = -in_file_length
        in_file.seek(seek_offset, os.SEEK_END)
        buf = in_file.read(block_size)

        # Search for line end.
        if ignore_ending_newline and seek_offset == -block_size and buf[-1] == '\n':
            buf = buf[:-1]
        pos = buf.rfind('\n')
        if pos != -1:
            # Found line end.
            return buf[pos+1:] + suffix

        suffix = buf + suffix

    # One-line file.
    return suffix