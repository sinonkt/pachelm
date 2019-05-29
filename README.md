
# activate env
conda activate python-pacyhderm

# to install python-pachyderm
conda install -y conda-build click setuptools-git


# Work around
<!-- - pip:
  - "--editable=git+https://github.com/pachyderm/python-pachyderm.git@faaea725837a2deecfe99b28d65dd98342c9e939#egg=python-pachyderm-master" -->

# Recommended way but not work.
# conda skeleton pypi python-pachyderm
# conda build python-pachyderm

# Next things
# Build pachadm, dev examples workflow for both contribute & own work.
# python-pachyderm get version mismatch for some method like pfs_client.provenances_for_repo('test-pipeline').

# pip install -e .


<!-- 2019-05-28T09:45:32Z INFO pps.API.CreatePipeline {"request":{"pipeline":{"name":"test-pipeline"}}} []
2019-05-28T09:45:32Z INFO pps.API.call64 {"duration":0.000026444,"request":{"pipeline":{"name":"test-pipeline"},"salt":"5a738ec86ad04489bd2e25d97bb2e716"},"response":null} []
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x1 addr=0x8 pc=0x1ce205d] -->