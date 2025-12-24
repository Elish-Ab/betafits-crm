"""LLM prompt templates module."""

from lib.prompts.loader import (
    PromptLoader,
    PromptTemplate,
    get_classifier_prompt,
    get_entity_extractor_prompt,
    get_relation_extractor_prompt,
    get_response_drafter_prompt,
)

__all__ = [
    "PromptLoader",
    "PromptTemplate",
    "get_classifier_prompt",
    "get_entity_extractor_prompt",
    "get_relation_extractor_prompt",
    "get_response_drafter_prompt",
]
