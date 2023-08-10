# bump_version.py
filename = 'dinnovation/version.py'

# Read the current version
with open(filename, 'r') as file:
    lines = file.readlines()
    version_line = [line for line in lines if line.startswith('version')][0]
    current_version = version_line.split('=')[1].strip().strip("'")

# Split the version into parts
major, minor, patch = current_version.split('.')
minor = int(minor) + 1  # Increment the minor version by 1

# Convert the minor version back to two decimal places
minor = f"{minor / 100:.02f}".split('.')[1]

# Create the new version
new_version = f"{major}.{minor}.{patch}"

# Replace the version in the file
with open(filename, 'w') as file:
    for line in lines:
        if line.startswith('version'):
            file.write(f"version = '{new_version}'\n")
        else:
            file.write(line)

print(f"Bumped version from {current_version} to {new_version}")
