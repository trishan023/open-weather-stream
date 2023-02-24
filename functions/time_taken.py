from datetime import datetime as dt

# Function to keep time (because these things can take forever to run)
def time_taken(time: dt) -> str:
    s = (dt.now()-time).total_seconds()
    
    if s < 60:
        return f'{s} sec'
    else:
        m = int(s/60)
        s = int(s%60)
    
    if m < 60:
        return f'{m} min {s} sec'
    else:
        h = int(m/60)
        m = int(m%60)
        return f'{h} hr {m} min {s} sec'