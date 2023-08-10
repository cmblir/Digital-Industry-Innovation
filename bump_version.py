# bump_version.py
filename = 'dinnovation/version.py'

# Read the current version
with open(filename, 'r') as file:
    lines = file.readlines()
    version_line = [line for line in lines if line.startswith('version')][0]
    current_version = version_line.split('=')[1].strip().strip("'")
    current_version = current_version.replace('"', "")

# Split the version into parts
major, minor, patch, micro = current_version.split('.')
micro = int(micro) + 1  # Increment the micro version by 1

# Create the new version
new_version = f"{major}.{minor}.{patch}.{micro}"

# Replace the version in the file
with open(filename, 'w') as file:
    for line in lines:
        if line.startswith('version'):
            file.write(f"version = '{new_version}'\n")
        else:
            file.write(line)

print(f"Bumped version from {current_version} to {new_version}")
