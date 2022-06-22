cd ~
export CONDAPATH="$(pwd)/miniconda3"
export PYTHON="$(pwd)/miniconda3/envs/hummingbot-professional/python3"
# Clone Hummingbot
git clone https://github.com/hummingbot-professional/hummingbot.git
# Install Hummingbot
export hummingbotPath="$(pwd)/hummingbot-professional" && cd $hummingbotPath && ./install
# Activate environment and compile code
conda activate hummingbot && ./compile
# Start Hummingbot 
winpty python bin/hummingbot.py