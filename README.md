# ProtectionDifferentialPitch
A pitch based on differential comparisons between non-protected monopoly industries and non-protected competitive industries.

## Motivation
[The return to protectionism](http://www.econ.ucla.edu/pfajgelbaum/RTP.pdf)
or [Paul Krugman's summary](https://www.gc.cuny.edu/CUNY_GC/media/LISCenter/pkrugman/TARIFFS.pdf)
for the motivation behind this pitch.

## Things to improve
### Coding
 - County boundaries are hard borders. Make these more flexible with a falloff over county distances.
 - No validation years nor mechanism (would be nice to validate on 2000 steel tariffs that employment change occurs).
 - Make sectoral data come with its year to facilitate downloading the proper survey dataset for businesses.
### Non-Coding
 - Sectoral data is incomplete. Try to improve.
 - Add sectoral data for other events.
 - Loss function is poor. Try to adjust `county_loss_func`
 (you'll want `penalty` to reflect how indicative of the trend described in the papers the current county (row) is. Higher is worse)
