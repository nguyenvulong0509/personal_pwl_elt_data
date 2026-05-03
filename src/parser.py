def safe_get(row, index):
    """
    Helper to safely grab a column value without throwing an IndexError.
    Returns None if the value is missing or completely empty.
    """
    if index < len(row):
        val = str(row[index]).strip()
        return val if val else None
    return None

def parse_workout_sheet_data(sheet_data, default_trainee=None, default_age=None, default_date=None):
    """
    Reads messy Workout Plan data (list of lists from Google Sheets API) row by row.
    Returns a clean, flat list of dictionaries representing each set/exercise.
    """
    parsed_data = []
    
    # --- State Variables (The "Memory" of the parser) ---
    trainee_name = default_trainee
    trainee_age = default_age
    date_range = default_date
    current_week = None
    current_day = None
    current_section = None  # Tracks if we are in 'warmup', 'workout', or 'cooldown'
    
    for row in sheet_data:
        # Skip completely empty rows
        if not row or all(str(cell).strip() == '' for cell in row):
            continue
            
        col_0 = safe_get(row, 0)
        col_1 = safe_get(row, 1)
        
        # Create lowercase versions for robust case-insensitive matching
        col_0_lower = col_0.lower() if col_0 else ''
        col_1_lower = col_1.lower() if col_1 else ''
        
        # Clean up strings to handle missing colons or extra spaces
        col_0_clean = col_0_lower.replace(':', '').strip()
        col_1_clean = col_1_lower.replace(':', '').strip()
        
        # --- 1. Catch Metadata (Trainee, Date, Week) ---
        match_clean = None
        val_col = 1
        
        if col_0_clean in ['trainee', 'học viên', 'age', 'tuổi', 'date', 'ngày', 'week', 'tuần']:
            match_clean = col_0_clean
            val_col = 1
        elif col_1_clean in ['trainee', 'học viên', 'age', 'tuổi', 'date', 'ngày', 'week', 'tuần']:
            match_clean = col_1_clean
            val_col = 2
            
        if match_clean:
            if match_clean in ['trainee', 'học viên']:
                trainee_name = safe_get(row, val_col)
            elif match_clean in ['age', 'tuổi']:
                trainee_age = safe_get(row, val_col)
            elif match_clean in ['date', 'ngày']:
                # Handle date ranges spanning across multiple columns
                val1 = safe_get(row, val_col)
                val2 = safe_get(row, val_col + 1)
                if val1 and val2:
                    date_range = f"{val1} - {val2}"
                else:
                    date_range = val1
            elif match_clean in ['week', 'tuần']:
                current_week = safe_get(row, val_col)
            continue
            
        # --- 2. Catch Section Changes ---
        if col_0_lower == 'trước tập':
            current_section = 'warmup'
            continue
        elif col_0_lower == 'sau tập':
            current_section = 'cooldown'
            continue
        elif col_0_lower.startswith('day '):
            current_day = col_0
            current_section = 'workout'
            # Skip if it says "Day 1, Off"
            if col_1_lower == 'off':
                continue 
            continue
            
        # --- 3. Extract the actual Workout Data ---
        if current_section == 'workout':
            # Ignore header rows and summary rows
            if col_0_lower == 'exercise' or col_1_lower == 'exercise' or 'total sets' in col_0_lower:
                continue
            
            # A valid exercise row MUST have an exercise name in column 1
            if col_1:
                exercise_data = {
                    'trainee_name': trainee_name,      
                    'trainee_age': trainee_age,
                    'date_range': date_range,          
                    'week_id': f"W{current_week}" if current_week else None,
                    'day_name': current_day,
                    'exercise_id': col_0,
                    'exercise_name': col_1,
                    'prescribed_sets': safe_get(row, 2),
                    'prescribed_reps': safe_get(row, 3),
                    'target_rpe': safe_get(row, 4),
                    'target_rm_pct': safe_get(row, 5),
                    'tempo': safe_get(row, 6),
                    'method': safe_get(row, 7),
                    'actual_load': safe_get(row, 8),
                    'rest_time': safe_get(row, 9),
                    'coach_note': safe_get(row, 10),
                    'actual_rpe': safe_get(row, 11),
                    'client_note': safe_get(row, 12),
                    'mental_state': safe_get(row, 13)
                }
                parsed_data.append(exercise_data)
                
    return parsed_data, trainee_name, trainee_age, date_range
