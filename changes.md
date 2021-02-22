# Capturing change

## Challenge

The current changes implementation is quite poor.

- it relies on every change being captured on a daily basis
- not al changes are actually captured
- you still have to go to every commodity code anyway

## What we need to capture and in what circumstances

### Measures

Any change to:

- measure
- measure component
- measure condition
- measure condition component
- measure excluded geographical area
- measure footnote association

Needs to trigger a change be loaded

Question - what if all of the changes are at chapter level, this means that all comm codes need to be updated anyway