# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

## Claude Code specifics

A Stop hook in `.claude/settings.json` runs the quality gates (`just format && just lint-fix && just type-check && just test-unit`) automatically when a turn ends.
