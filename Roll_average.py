#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
****************************
Created with IVAN
@author: skotsan
"""

import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt

# Set mean and standard deviation
mean = 100
std_dev = 20

# Generate x-axis values
x = np.linspace(0, 260, 500)

# Generate normal distribution
y = norm.pdf(x, mean, std_dev)

# Scale the y-axis to have a peak at 210
y *= 210 / y.max()

# Plot the chart
plt.plot(x, y, label='Normal Distribution')
plt.xlabel('Value')
plt.ylabel('Density')
plt.legend()
plt.show()