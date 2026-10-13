"""Reviewed Oct 5 metadata enrollment operator, now connected to durable execution.

These helpers retain the reviewed operations, with an optional shorter outer deadline. They
only publish proven NULL locators and enroll absent markers. Chunk clearing is
owned by retirement_runner and its unchanged full archive verifier.
"""
