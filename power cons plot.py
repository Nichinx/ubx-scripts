import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

df = pd.read_csv("data3.csv")
df.sort_values(["ts"], axis=0, ascending=[True], inplace=True)
df['ts'] = df['ts'].astype(str).apply(lambda x: datetime.strptime(x, '%y%m%d%H%M%S'))
# df = df.loc[(df['power'] > 148)]
# df_one_cycle = df.loc[(df.ts >= '2023-08-23 18:29:31') & (df.ts <= '2023-08-23 18:39:28')]
# df_uptime = df.loc[(df.ts >= '2023-08-23 18:29:31') & (df.ts <= '2023-08-23 18:30:06')]

# fig, ax = plt.subplots()
fig, axs = plt.subplots(3, 1, sharex=True)
fig.suptitle('Power Consumption', fontweight='bold')

# plt.plot(df_one_cycle.ts, df_one_cycle.power, "red")
# plt.title('v5_rover - once cycle', loc='center', color="black")
# plt.ylabel('Power, mW')

# plt.plot(df_uptime.ts, df_uptime.power, "blue")
# plt.title('v5_rover - uptime', loc='center', color="black")
# plt.ylabel('Power, mW')

# Plot the data
axs[0].plot(df.ts, df.load_voltage, "blue",  label='Load Voltage')
axs[0].set_ylabel('load_voltage, V')
axs[0].legend()

axs[1].plot(df.ts, df.current, "green", label='Current')
axs[1].set_ylabel('current, mA')
axs[1].legend()

axs[2].plot(df.ts, df.power, "red", label='Power')
axs[2].set_ylabel('power, mW')
axs[2].legend()

plt.xlabel('ts')
plt.show()