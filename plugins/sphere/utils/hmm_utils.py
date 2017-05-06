# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import numpy as np
from scipy.stats import multivariate_normal
from hmmlearn.hmm import _BaseHMM, _validate_covars

### MK: the following has been first copied from hmm.GaussianHMM?? of hmmlearn and then modified
# TODO: Eliminate unnecessary ingredients

class RSSI_HMM(_BaseHMM):
    """Hidden Markov Model with Bernoulli missingness and Gaussian emissions.

    Parameters
    ----------
    n_components : int
        Number of states.

    min_covar : float
        Floor on the diagonal of the covariance matrix to prevent
        overfitting. Defaults to 1e-3.

    startprob_prior : array, shape (n_components, )
        Initial state occupation prior distribution.

    transmat_prior : array, shape (n_components, n_components)
        Matrix of prior transition probabilities between states.

    algorithm : string
        Decoder algorithm. Must be one of "viterbi" or "map".
        Defaults to "viterbi".

    random_state: RandomState or an int seed
        A random number generator instance.

    n_iter : int, optional
        Maximum number of iterations to perform.

    tol : float, optional
        Convergence threshold. EM will stop if the gain in log-likelihood
        is below this value.

    verbose : bool, optional
        When ``True`` per-iteration convergence reports are printed
        to :data:`sys.stderr`. You can diagnose convergence via the
        :attr:`monitor_` attribute.

    params : string, optional
        Controls which parameters are updated in the training
        process.  Can contain any combination of 's' for startprob,
        't' for transmat, 'm' for means and 'c' for covars. Defaults
        to all parameters.

    init_params : string, optional
        Controls which parameters are initialized prior to
        training.  Can contain any combination of 's' for
        startprob, 't' for transmat, 'm' for means and 'c' for covars.
        Defaults to all parameters.

    Attributes
    ----------
    n_features : int
        Dimensionality of the Gaussian emissions.

    monitor\_ : ConvergenceMonitor
        Monitor object used to check the convergence of EM.

    transmat\_ : array, shape (n_components, n_components)
        Matrix of transition probabilities between states.

    startprob\_ : array, shape (n_components, )
        Initial state occupation distribution.

    means\_ : array, shape (n_components, n_features)
        Mean parameters for each state.

    covars\_ : array
        Covariance parameters for each state with shape:
            (n_components, n_features, n_features)

    """

    def __init__(self, n_components=1,
                 min_covar=1e-3,
                 startprob_prior=1.0, transmat_prior=1.0,
                 means_prior=0, means_weight=0,
                 covars_prior=1e-2, covars_weight=1,
                 miss_probs_prior=0.5, miss_probs_weight=1, ### what does the weight mean?
                 algorithm="viterbi", random_state=None,
                 n_iter=10, tol=1e-2, verbose=False,
                 params="stmc", init_params="stmc"):
        _BaseHMM.__init__(self, n_components,
                          startprob_prior=startprob_prior,
                          transmat_prior=transmat_prior, algorithm=algorithm,
                          random_state=random_state, n_iter=n_iter,
                          tol=tol, params=params, verbose=verbose,
                          init_params=init_params)

        self.min_covar = min_covar
        self.means_prior = means_prior
        self.means_weight = means_weight
        self.covars_prior = covars_prior
        self.covars_weight = covars_weight
        self.miss_probs_prior = miss_probs_prior
        self.miss_probs_weight = miss_probs_weight

    def _check(self):
        super(RSSI_HMM, self)._check()

        self.means_ = np.asarray(self.means_)
        self.n_features = self.means_.shape[1]
        self.miss_probs_ = np.asarray(self.miss_probs_)

        _validate_covars(self.covars_, 'full', self.n_components)

    def _init(self, X, lengths=None):
        super(RSSI_HMM, self)._init(X, lengths=lengths)

        _, n_features = X.shape
        if hasattr(self, 'n_features') and self.n_features != n_features:
            raise ValueError('Unexpected number of dimensions, got %s but '
                             'expected %s' % (n_features, self.n_features))

        self.n_features = n_features

    def _compute_log_likelihood(self, X):
        seq_len = X.shape[0]
        n_states = self.n_components
        n_dim = X.shape[1]
        p = np.zeros((seq_len,n_states))
        for i in range(seq_len):
            miss = np.isnan(X[i])
            p[i] = np.sum(miss * np.log(self.miss_probs_) + (1-miss) * np.log(1-self.miss_probs_), axis=1)
            if not np.all(miss):
                for state in range(n_states):
                    mean = self.means_[state][miss==0]
                    cov = self.covars_[state][np.ix_(miss==0,miss==0)]
                    p[i][state] = p[i][state] + np.log(multivariate_normal.pdf(X[i][miss==0],mean=mean,cov=cov))
        return p

    def _generate_sample_from_state(self, state, random_state=None):
        cv = self.covars_[state]
        x = np.random.multivariate_normal(self.means_[state], cv)
        miss = np.random.binomial(1,self.miss_probs_[state],size=x.shape)
        x[miss==1] = np.nan
        return x

    def decode(self, X, lengths=None, algorithm=None):
        XX = X.copy()
        XX[np.isnan(XX)] = -1e100
        return super(RSSI_HMM, self).decode(XX, lengths=lengths, algorithm=algorithm)

    def _decode_viterbi(self, X):
        XX = X.copy()
        XX[XX==-1e100] = np.nan
        return super(RSSI_HMM, self)._decode_viterbi(XX)




# np.random.seed(42)
# model = RSSI_HMM(n_components=4)
# model.startprob_ = np.array([0.6, 0.2, 0.1, 0.1])
# model.transmat_ = np.array([[0.7, 0.1, 0.1, 0.1],
#                             [0.3, 0.4, 0.2, 0.1],
#                             [0.3, 0.2, 0.4, 0.1],
#                             [0.2, 0.2, 0.2, 0.4]])
# model.means_ = np.array([[0.0, 0.0, 0.0], [3.0, -3.0, 0.0], [5.0, 10.0, 0.0], [-3.0, -3.0, -3.0]])
# model.covars_ = np.tile(np.identity(3), (4, 1, 1))
# model.miss_probs_ = np.array([[0.1,0.3,0.5],[0.6,0.4,0.2],[0.1,0.1,0.1],[0.9,0.9,0.9]])
# X, Z = model.sample(100)
# res = model._compute_log_likelihood(X)
# Z2 = model.predict(X)
# print(Z)
# print(Z2)
# print(0+(Z==Z2))
