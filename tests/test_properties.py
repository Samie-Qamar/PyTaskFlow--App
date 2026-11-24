"""Property-based tests with Hypothesis."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from pytaskflow.models import Job, JobStatus
from pytaskflow.store import JobStore
from pytaskflow.tasks import word_count

words = st.text(alphabet=st.characters(categories=("Ll", "Lu", "Nd")), min_size=1, max_size=8)


@given(st.lists(words, max_size=50))
def test_word_count_total_equals_number_of_words(ws: list[str]) -> None:
    assert sum(word_count(" ".join(ws)).values()) == len(ws)


@given(st.text())
def test_word_count_is_case_insensitive(text: str) -> None:
    assert word_count(text.upper()) == word_count(text.upper().lower())
    assert all(k == k.lower() for k in word_count(text))


@given(st.lists(st.sampled_from(list(JobStatus)), max_size=30))
def test_counts_always_match_listing(statuses: list[JobStatus]) -> None:
    store = JobStore()
    for s in statuses:
        job = store.add(Job(task="sleep", payload={}))
        store.update(job.id, lambda j, s=s: setattr(j, "status", s))
    counts = store.counts()
    assert sum(counts.values()) == len(statuses)
    for s in JobStatus:
        assert counts[s.value] == len(store.list(s))
