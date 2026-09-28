"""R2 chronological state and metrics. No truth enters Decoder."""
import math

class Decoder:
    def __init__(self, on, off, confirm=2):
        if not 0 <= off <= on <= 1 or confirm not in (1, 2):
            raise ValueError('Invalid threshold policy')
        self.on, self.off, self.confirm = on, off, confirm
        self.reset()

    def reset(self):
        self.state = self.previous = -1

    def step(self, score):
        if score is None or not math.isfinite(score):
            self.reset()
            return -1
        evidence = 1 if score >= self.on else 0 if (score < self.off if self.on == self.off else score <= self.off) else -1
        if evidence >= 0 and (self.confirm == 1 or self.previous == evidence):
            self.state = evidence
        self.previous = evidence
        return self.state

def evaluate(truth, states):
    if len(truth) != len(states):
        raise ValueError('Length mismatch')
    c = dict.fromkeys(('tp', 'fp', 'fn', 'tn', 'u_on', 'u_off'), 0)
    keys = {(1, 1): 'tp', (0, 1): 'fp', (1, 0): 'fn', (0, 0): 'tn', (1, -1): 'u_on', (0, -1): 'u_off'}
    for y, s in zip(truth, states):
        if y not in (-1, 0, 1) or s not in (-1, 0, 1):
            raise ValueError('Invalid label/state')
        if y >= 0:
            c[keys[y, s]] += 1
    def ratio(a, b): return a / b if b else None
    p = ratio(c['tp'], c['tp'] + c['fp'])
    r = ratio(c['tp'], c['tp'] + c['fn'] + c['u_on'])
    u = ratio(c['u_on'] + c['u_off'], sum(c.values()))
    return dict(counts=c, precision=p, recall_including_unknown=r, unknown_fraction=u,
                conditional_recall=ratio(c['tp'], c['tp'] + c['fn']),
                rank_score=ratio(2*c['tp'], 2*c['tp']+c['fp']+c['fn']+c['u_on']))

def true_on_events(labels):
    """Half-open runs; unknown and edges are censored, never inferred OFF."""
    start = None
    for i in range(len(labels)+1):
        on = i < len(labels) and labels[i] == 1
        if on and start is None:
            start = i
        elif not on and start is not None:
            yield dict(start=start, end=i, duration=i-start,
                       left_censored=bool(start == 0 or labels[start-1] != 0),
                       right_censored=bool(i == len(labels) or labels[i] != 0))
            start = None

def trace_complete(labels, states):
    """Both true transitions and an estimated OFF→ON→OFF cycle are required."""
    for event in true_on_events(labels):
        if event['left_censored'] or event['right_censored']:
            continue
        seen_off = hit = False
        for i, state in enumerate(states):
            if state < 0:
                seen_off = hit = False
            elif state == 0:
                if hit: return True
                seen_off = True
            elif seen_off and event['start'] <= i < event['end']:
                hit = True
    return False

def quality_flags(metrics, complete):
    p, r, u = (metrics[k] for k in ('precision', 'recall_including_unknown', 'unknown_fraction'))
    return {name: bool(complete and p is not None and r is not None and u is not None and p >= limit and r >= limit and u <= ul)
            for name, limit, ul in [('Q80', .8, .05), ('Q90', .9, .01)]}
