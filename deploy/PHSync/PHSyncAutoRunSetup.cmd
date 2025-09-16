if not exist C:\Ziitech mkdir C:\Ziitech

SCHTASKS /CREATE /SC DAILY /TN "ZiiPOSPublicHolidaySync" /TR "'C:\Ziitech\PHSync\ZiiPOS_PublicHolidaySync.exe'" /ST 01:00 /RL HIGHEST
exit